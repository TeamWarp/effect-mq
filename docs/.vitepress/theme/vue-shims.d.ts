// SFC module shim for the editor; vitepress compiles .vue files itself.
declare module "*.vue" {
  import type { DefineComponent } from "vue"
  const component: DefineComponent
  export default component
}
