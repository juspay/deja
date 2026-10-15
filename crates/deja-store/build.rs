fn main() {
    // sqlx::migrate! embeds directory contents at compile time. Cargo does not
    // otherwise rebuild the crate when a new migration file is added.
    println!("cargo:rerun-if-changed=migrations");
}
