use proc_macro2::TokenStream;
use syn::ItemFn;

pub type BoundaryArgs = crate::instrument::InstrumentArgs;

pub fn generate(args: BoundaryArgs, func: ItemFn) -> TokenStream {
    crate::instrument::generate_with_boundary(args, func, Some("function"))
}
