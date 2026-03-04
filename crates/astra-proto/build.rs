use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                "proto/rpc.proto",
                "proto/mvccpb/kv.proto",
                "proto/raft.proto",
                "proto/admin.proto",
            ],
            &["proto"],
        )?;

    Ok(())
}
