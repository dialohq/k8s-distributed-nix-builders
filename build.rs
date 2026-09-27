fn main() {
    tonic_prost_build::compile_protos("proto/collection.proto").expect("compile collection RPC schema");
    println!("cargo:rerun-if-changed=proto/collection.proto");
    println!("cargo:rerun-if-changed=native/nix_bridge.cc");
    println!("cargo:rerun-if-changed=native/nix_bridge.h");
    let mut cpp = cc::Build::new();
    // Nix's development environment enables _FORTIFY_SOURCE, which needs -O1+.
    if std::env::var("OPT_LEVEL").as_deref() == Ok("0") {
        cpp.opt_level(1);
    }
    cpp.cpp(true).std("c++23").file("native/nix_bridge.cc");
    // Compile first, then emit dynamic Nix link libraries after the static shim.
    for package in ["nix-main", "nix-store", "nlohmann_json", "sqlite3"] {
        let lib = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(package)
            .unwrap_or_else(|e| panic!("missing {package}; build with the root flake distributed-nix package: {e}"));
        for dir in lib.include_paths {
            cpp.include(dir);
        }
        for (name, value) in lib.defines {
            cpp.define(&name, value.as_deref());
        }
    }
    cpp.compile("distributed_nix_bridge");
    for package in ["nix-main", "nix-store", "sqlite3"] {
        pkg_config::Config::new()
            .probe(package)
            .expect("Nix libraries");
    }
}
