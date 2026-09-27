#include "nix_bridge.h"
#include "nix/main/shared.hh"
#include "nix/util/callback.hh"
#include "nix/util/signals.hh"
#include <chrono>
#include "nix/store/local-store.hh"
#include "nix/store/gc-store.hh"
#include "nix/store/path-info.hh"
#include "nix/store/store-api.hh"
#include "nix/store/store-open.hh"
#include "nix/store/daemon.hh"
#include "nix/store/globals.hh"
#include "nix/store/realisation.hh"
#include "nix/store/derivations.hh"
#include <functional>
#include <memory>
#include <thread>
#include <sys/socket.h>
#include <nlohmann/json.hpp>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string_view>

namespace {
std::mutex nix_mutex;
std::once_flag initialized;

nlohmann::json runtime(uint32_t operation, const nlohmann::json & request)
{
    auto bytes = request.dump();
    distributed_nix_buffer result{nullptr, 0};
    auto status = distributed_nix_runtime_v1(operation,
        reinterpret_cast<const unsigned char *>(bytes.data()), bytes.size(), &result);
    std::string text(reinterpret_cast<const char *>(result.data), result.len);
    distributed_nix_buffer_free_v1(&result);
    if (status) throw nix::Error("shared store: %s", text);
    return nlohmann::json::parse(text);
}

void pinClientPaths(const std::vector<std::string> & paths)
{
    while (true) {
        nix::checkInterrupt();
        if (runtime(5, paths).get<bool>()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
}

// Nix still owns the wire protocol, evaluator-facing API, build engine and SQL.
// Every registration (build, source import, copy, substitution) gets an outbox
// record BEFORE the native transaction. Invalid records are harmless and retryable.
class SharedLocalStore : public nix::LocalStore {
public:
    explicit SharedLocalStore(nix::ref<const nix::LocalStoreConfig> config)
        : nix::Store(*config), nix::LocalFSStore(*config), nix::LocalStore(config) {}

    bool isValidPathUncached(const nix::StorePath & path) override
    {
        pinClientPaths(std::vector<std::string>{printStorePath(path)});
        return nix::LocalStore::isValidPathUncached(path);
    }

    void queryPathInfoUncached(const nix::StorePath & path,
        nix::Callback<std::shared_ptr<const nix::ValidPathInfo>> callback) noexcept override
    {
        try { pinClientPaths(std::vector<std::string>{printStorePath(path)}); }
        catch (...) { callback.rethrow(); return; }
        nix::LocalStore::queryPathInfoUncached(path, std::move(callback));
    }

    void addTempRoot(const nix::StorePath & path) override
    {
        pinClientPaths(std::vector<std::string>{printStorePath(path)});
        nix::LocalStore::addTempRoot(path);
    }

    void registerValidPaths(const nix::ValidPathInfos & infos) override
    {
        std::vector<std::string> pins;
        for (const auto & [path, info] : infos) {
            pins.push_back(printStorePath(path));
            for (const auto & ref : info.references) pins.push_back(printStorePath(ref));
            if (info.deriver) pins.push_back(printStorePath(*info.deriver));
        }
        pinClientPaths(pins);
        std::vector<std::string> paths;
        for (const auto & [path, info] : infos)
            if (!isValidPath(path)) paths.push_back(printStorePath(path));
        if (!paths.empty()) runtime(1, paths);
        nix::LocalStore::registerValidPaths(infos);
        if (!paths.empty()) runtime(3, nullptr);
    }

    void registerDrvOutput(const nix::Realisation & info) override
    {
        std::vector<std::string> pins{printStorePath(info.outPath)};
        for (const auto & [id, path] : info.dependentRealisations) pins.push_back(printStorePath(path));
        pinClientPaths(pins);
        auto old = queryRealisation(info.id);
        runtime(4, nlohmann::json(old ? nix::Realisation{*old, info.id} : info));
        nix::LocalStore::registerDrvOutput(info);
    }

    void collectGarbage(const nix::GCOptions &, nix::GCResults &) override
    {
        throw nix::Error("shared store GC requires the administrator coordinator");
    }

};


int reply(distributed_nix_buffer *out, std::string_view text, int status) noexcept
{
    if (text.empty()) return status;
    auto *data = static_cast<unsigned char *>(std::malloc(text.size()));
    if (!data) return 2;
    std::memcpy(data, text.data(), text.size());
    out->data = data;
    out->len = text.size();
    return status;
}

class TransferStore : public nix::LocalStore {
public:
    explicit TransferStore(nix::ref<const nix::LocalStoreConfig> config)
        : nix::Store(*config), nix::LocalFSStore(*config), nix::LocalStore(config) {}

    void collectGarbage(const nix::GCOptions &, nix::GCResults &) override
    {
        throw nix::Error("shared store GC requires the administrator coordinator");
    }

    void optimiseStore() override
    {
        throw nix::Error("transfer endpoint does not optimise the store");
    }

    void repairPath(const nix::StorePath &) override
    {
        throw nix::Error("cannot repair a live shared collection");
    }

    void buildPaths(const std::vector<nix::DerivedPath> &, nix::BuildMode,
        std::shared_ptr<nix::Store>) override
    {
        throw nix::Error("collection endpoint does not execute builds");
    }

    std::vector<nix::KeyedBuildResult> buildPathsWithResults(
        const std::vector<nix::DerivedPath> &, nix::BuildMode, std::shared_ptr<nix::Store>) override
    {
        throw nix::Error("collection endpoint does not execute builds");
    }

    nix::BuildResult buildDerivation(const nix::StorePath &, const nix::BasicDerivation &,
        nix::BuildMode) override
    {
        throw nix::Error("collection endpoint does not execute builds");
    }

    nix::StorePath addToStoreFromDump(nix::Source &, std::string_view,
        nix::FileSerialisationMethod, nix::ContentAddressMethod, nix::HashAlgorithm,
        const nix::StorePathSet &, nix::RepairFlag) override
    {
        throw nix::Error("collection uploads require path metadata");
    }

    void addToStore(const nix::ValidPathInfo & info, nix::Source & source,
        nix::RepairFlag repair, nix::CheckSigsFlag checkSigs) override
    {
        if (repair != nix::NoRepair) throw nix::Error("cannot replace a live shared path");
        nix::settings.fsyncStorePaths = true;
        nix::LocalStore::addToStore(info, source, repair, checkSigs);
    }
};

nlohmann::json invoke(uint32_t op, const char *uri, const nlohmann::json &input)
{
    if (op < DISTRIBUTED_NIX_DUMP || op > 14)
        throw std::invalid_argument("unknown Nix bridge operation");
    std::call_once(initialized, [] { nix::initNix(); });
    auto store = nix::openStore(uri);
    if (op == 13) {
        nlohmann::json conflicts = nlohmann::json::array();
        for (const auto & [id, value] : input.items()) {
            auto record = value.get<nix::Realisation>();
            if (id != record.id.to_string()) throw nix::Error("realisation ID mismatch");
            if (auto old = store->queryRealisation(record.id); old && !record.isCompatibleWith(*old))
                conflicts.push_back(id);
        }
        return conflicts;
    }
    if (op == 12) {
        auto target = nix::openStore(input.at("target").get<std::string>());
        nix::StorePathSet roots, closure;
        for (const auto & path : input.at("paths")) roots.insert(store->parseStorePath(path.get<std::string>()));
        store->computeFSClosure(roots, closure);
        nix::copyPaths(*store, *target, closure, nix::NoRepair, nix::NoCheckSigs, nix::NoSubstitute);
        return {{"paths", closure.size()}};
    }
    if (op == 9) {
        auto result = input;
        result["paths"] = nlohmann::json::object();
        auto pending = input.at("roots").get<std::vector<std::string>>();
        while (!pending.empty()) {
            auto name = pending.back(); pending.pop_back();
            if (result["paths"].contains(name)) continue;
            auto path = store->parseStorePath(name);
            nlohmann::json info;
            if (store->isValidPath(path)) {
                auto old = store->queryPathInfo(path);
                if (input.at("paths").contains(name)) {
                    auto proposed = nix::UnkeyedValidPathInfo::fromJSON(&store->config, input.at("paths").at(name));
                    if ((old->ca || proposed.ca) && (old->narHash != proposed.narHash || old->narSize != proposed.narSize || old->references != proposed.references || old->ca != proposed.ca))
                        throw nix::Error("conflicting content-addressed path '%s'", name);
                }
                info = old->toJSON(&store->config, true, nix::PathInfoJsonFormat::V1);
            } else info = input.at("paths").at(name);
            for (const auto & ref : info.at("references")) pending.push_back(ref.get<std::string>());
            result["paths"][name] = std::move(info);
        }
        return result;
    }
    if (op == 8) {
        nlohmann::json result = nlohmann::json::array();
        for (const auto & path : store->queryAllValidPaths()) {
            if (!path.isDerivation()) continue;
            auto drv = store->readDerivation(path);
            if (!drv.type().isCA()) continue;
            for (const auto & [name, hash] : nix::staticOutputHashes(*store, drv)) {
                nix::DrvOutput id{hash, name};
                if (auto r = store->queryRealisation(id))
                    result.push_back(nix::Realisation{*r, id});
            }
        }
        return result;
    }
    if (op == 6) {
        std::vector<std::string> valid;
        for (const auto & p : input.get<std::vector<std::string>>())
            if (store->isValidPath(store->parseStorePath(p))) valid.push_back(p);
        return valid;
    }
    if (op == DISTRIBUTED_NIX_DUMP || op == 7) {
        nix::StorePathSet roots, closure;
        std::set<nix::Realisation> realisations;
        nlohmann::json ready = nlohmann::json::array();
        if (op == 7) {
            for (const auto & value : input) {
                auto pending = value.get<nix::Realisation>();
                if (auto r = store->queryRealisation(pending.id)) {
                    if (!store->isValidPath(r->outPath)) continue;
                    realisations.insert(nix::Realisation{*r, pending.id});
                    ready.push_back(pending.id.to_string());
                }
            }
            if (realisations.empty()) return nullptr;
            realisations = nix::Realisation::closure(*store, realisations);
            for (const auto & r : realisations) roots.insert(r.outPath);
        } else {
            for (const auto &p : input.get<std::vector<std::string>>())
                roots.insert(store->parseStorePath(p));
        }
        if (roots.empty()) throw std::invalid_argument("empty closure roots");
        store->computeFSClosure(roots, closure);
        nlohmann::json result = {{"version", 1}, {"paths", nlohmann::json::object()}, {"roots", nlohmann::json::array()}};
        for (const auto &p : roots) result["roots"].push_back(store->printStorePath(p));
        for (const auto &p : closure)
            result["paths"][store->printStorePath(p)] = store->queryPathInfo(p)->toJSON(&store->config, true, nix::PathInfoJsonFormat::V1);
        if (op == 7) {
            for (const auto & r : realisations) result["realisations"][r.id.to_string()] = r;
            return {{"manifest", result}, {"ready", ready}};
        }
        return result;
    }
    auto *local = dynamic_cast<nix::LocalStore *>(&*store);
    if (!local) throw nix::Error("admission requires a native local store");
    if (op == DISTRIBUTED_NIX_GC_SNAPSHOT || op == 14) {
        nix::GCOptions options; options.action = nix::GCAction::gcReturnLive;
        nix::GCResults live;
        if (op == 14) {
            auto prefix = local->config->stateDir.get() + "/gcroots/distributed-nix/";
            for (const auto & [path, sources] : local->findRoots(false))
                for (const auto & source : sources)
                    if (!source.starts_with(prefix)) { live.paths.insert(store->printStorePath(path)); break; }
        } else local->collectGarbage(options, live);
        auto paths = store->queryAllValidPaths();
        std::map<std::string, std::set<std::string>> graph;
        for (const auto &p : paths) {
            auto name = store->printStorePath(p);
            auto info = store->queryPathInfo(p);
            auto &edges = graph[name];
            for (const auto &ref : info->references) edges.insert(store->printStorePath(ref));
            // CA outputs need not appear in the static DerivationOutputs table.
            if (info->deriver && store->isValidPath(*info->deriver)) {
                auto d = store->printStorePath(*info->deriver);
                edges.insert(d); graph[d].insert(name);
            }
            // Conservatively retain valid derivations and their outputs together.
            // This also covers either native keep-derivations/keep-outputs setting.
            for (const auto &drv : store->queryValidDerivers(p)) {
                auto d = store->printStorePath(drv);
                edges.insert(d); graph[d].insert(name);
            }
        }
        return {{"live", live.paths}, {"graph", graph}};
    }
    if (op == DISTRIBUTED_NIX_GC_DELETE) {
        nix::GCOptions options; options.action = nix::GCAction::gcDeleteSpecific;
        // Native reachability enforcement is always on. Never ignore liveness.
        for (const auto &p : input.get<std::vector<std::string>>()) {
            auto path = store->parseStorePath(p);
            // Also retry invalid paths: Nix may have invalidated metadata before
            // a crash interrupted removal of the filesystem contents.
            options.pathsToDelete.insert(path);
        }
        nix::GCResults result; local->collectGarbage(options, result);
        for (const auto &path : options.pathsToDelete)
            if (store->isValidPath(path)) throw nix::Error("GC did not delete requested path '%s'", store->printStorePath(path));
        return {{"deleted", result.paths}, {"bytes_freed", result.bytesFreed}};
    }
    if (input.at("version") != 1) throw nix::Error("unsupported manifest version");
    nix::ValidPathInfos infos;
    nlohmann::json existing = nlohmann::json::array();
    auto preserveLocal = input.value("_preserveLocal", std::set<std::string>{});
    nlohmann::json localVariants = nlohmann::json::array();
    for (const auto &[path, json] : input.at("paths").items()) {
        nix::ValidPathInfo info(store->parseStorePath(path), nix::UnkeyedValidPathInfo::fromJSON(&store->config, json));
        if (info.ca && !info.isContentAddressed(store->config))
            throw nix::Error("invalid content-address metadata for '%s'", path);
        if (store->isValidPath(info.path)) {
            existing.push_back(path);
            auto old = store->queryPathInfo(info.path);
            if (old->narHash != info.narHash || old->narSize != info.narSize || old->references != info.references || old->ca != info.ca) {
                if (!preserveLocal.contains(path) || old->ca || info.ca)
                    throw nix::Error("conflicting already-registered path '%s'", path);
                info = *old;
                localVariants.push_back(path);
            }
            info.sigs.insert(old->sigs.begin(), old->sigs.end());
            info.ultimate = old->ultimate || info.ultimate;
        }
        infos.emplace(info.path, info);
    }
    std::map<nix::DrvOutput, nix::Realisation> realisations;
    auto realisationJSON = input.value("realisations", nlohmann::json::object());
    for (const auto & [id, value] : realisationJSON.items()) {
        auto r = value.get<nix::Realisation>();
        if (id != r.id.to_string() || !infos.contains(r.outPath))
            throw nix::Error("realisation is outside the manifest closure");
        if (auto old = store->queryRealisation(r.id); old && !r.isCompatibleWith(*old))
            throw nix::Error("conflicting realisation '%s'", id);
        realisations.emplace(r.id, r);
    }
    std::map<nix::DrvOutput, int> visited;
    std::vector<nix::DrvOutput> order;
    std::function<void(const nix::DrvOutput &)> visit = [&](const auto & id) {
        if (visited[id] == 2) return;
        if (visited[id] == 1) throw nix::Error("cyclic realisation dependencies");
        visited[id] = 1;
        const auto & r = realisations.at(id);
        for (const auto & [dep, path] : r.dependentRealisations) {
            if (!realisations.contains(dep) || realisations.at(dep).outPath != path)
                throw nix::Error("missing or conflicting realisation dependency");
            visit(dep);
        }
        visited[id] = 2;
        order.push_back(id);
    };
    for (const auto & [id, _] : realisations) visit(id);
    if (op == DISTRIBUTED_NIX_REGISTER) {
        local->registerValidPaths(infos);
        for (const auto & id : order) local->registerDrvOutput(realisations.at(id), nix::NoCheckSigs);
    }
    return {{"paths", infos.size()}, {"existing", existing}, {"local_variants", localVariants}, {"registered", op == DISTRIBUTED_NIX_REGISTER}};
}
}

extern "C" int distributed_nix_call_v1(uint32_t operation, const char *store,
    const unsigned char *input, size_t input_len, distributed_nix_buffer *result) noexcept
{
    if (!result) return 2;
    *result = {nullptr, 0};
    if (!store || !input) return reply(result, "null bridge argument", 2);
    try {
        // Keep Nix's global initialization and calls serialized inside the ABI,
        // including callers in languages other than Rust. Store handles stay local.
        std::lock_guard guard(nix_mutex);
        auto parsed = nlohmann::json::parse(input, input + input_len);
        return reply(result, invoke(operation, store, parsed).dump(), 0);
    } catch (const std::exception &e) {
        return reply(result, e.what(), 1);
    } catch (...) {
        return reply(result, "unknown C++ exception in Nix bridge", 1);
    }
}

extern "C" void distributed_nix_buffer_free_v1(distributed_nix_buffer *buffer) noexcept
{
    if (!buffer) return;
    std::free(buffer->data);
    *buffer = {nullptr, 0};
}

extern "C" int distributed_nix_serve_v1(int trusted, distributed_nix_buffer *result) noexcept
{
    if (!result) return 2;
    *result = {nullptr, 0};
    try {
        std::call_once(initialized, [] { nix::initNix(); });
        auto config = nix::make_ref<nix::LocalStoreConfig>("local", "",
            nix::StoreConfig::Params{{"path-info-cache-size", "0"}});
        auto store = nix::make_ref<SharedLocalStore>(config);
        nix::daemon::processConnection(store, nix::FdSource(0), nix::FdSink(1),
            trusted ? nix::Trusted : nix::NotTrusted, nix::daemon::NotRecursive);
        return 0;
    } catch (const std::exception & e) {
        return reply(result, e.what(), 1);
    } catch (...) {
        return reply(result, "unknown exception in native daemon", 1);
    }
}

extern "C" int distributed_nix_serve_transfer_v1(const char *uri, int trusted, distributed_nix_buffer *result) noexcept
{
    if (!result) return 2;
    *result = {nullptr, 0};
    if (!uri) return reply(result, "null store URI", 2);
    try {
        std::call_once(initialized, [] { nix::initNix(); });
        auto local = nix::openStore(uri).dynamic_pointer_cast<nix::LocalStore>();
        if (!local) throw nix::Error("collection requires a local store");
        auto store = nix::make_ref<TransferStore>(local->config);
        nix::daemon::processConnection(store, nix::FdSource(0), nix::FdSink(1),
            trusted ? nix::Trusted : nix::NotTrusted, nix::daemon::NotRecursive);
        return 0;
    } catch (const std::exception & e) {
        return reply(result, e.what(), 1);
    } catch (...) {
        return reply(result, "unknown exception in native collection daemon", 1);
    }
}
