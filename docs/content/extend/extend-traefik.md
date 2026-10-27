---
title: Extend Traefik
description: Extend Traefik with custom plugins using Yaegi and WebAssembly.
---

# Extend Traefik

Plugins are a powerful feature for extending Traefik with custom features and behaviors. The [Plugin Catalog](https://plugins.traefik.io/) is a software-as-a-service (SaaS) platform that provides an exhaustive list of the existing plugins.

??? note "Plugin Catalog Access"
    You can reach the [Plugin Catalog](https://plugins.traefik.io/) from the Traefik Dashboard using the `Plugins` menu entry.

## Add a new plugin to a Traefik instance

To add a new plugin to a Traefik instance, you must change that instance's install (static) configuration. Each plugin's **Install** section provides an install (static) configuration example. Many plugins have their own section in the Traefik routing (dynamic) configuration.

!!! danger "Experimental Features"
    Plugins can change the behavior of Traefik in unforeseen ways. Exercise caution when adding new plugins to production Traefik instances.

To learn more about how to add a new plugin to a Traefik instance, please refer to the [developer documentation](https://plugins.traefik.io/install).

## Plugin Systems

Traefik supports two different plugin systems, each designed for different use cases and developer preferences.

### Yaegi Plugin System

Traefik [Yaegi](https://github.com/traefik/yaegi) plugins are developed using the Go language. It is essentially a Go package. Unlike pre-compiled plugins, Yaegi plugins are executed on the fly by Yaegi, a Go interpreter embedded in Traefik.

This approach eliminates the need for compilation and a complex toolchain, making plugin development as straightforward as creating web browser extensions. Yaegi plugins support both middleware and provider functionality.

#### Key characteristics

- Written in Go language
- No compilation required
- Executed by embedded interpreter
- Supports full Go feature set
- Hot-reloadable during development

### WebAssembly (WASM) Plugin System

A Traefik WASM plugin is a [WebAssembly component](https://component-model.bytecodealliance.org/) that implements the [`wasi:http/middleware`](https://github.com/WebAssembly/WASI/tree/main/proposals/http) world of WASI 0.3. You can write it in any language with a toolchain for WASI 0.3 components, such as Rust with [`wit-bindgen`](https://github.com/bytecodealliance/wit-bindgen).

Traefik calls the exported `wasi:http/handler` interface for each request. The plugin forwards a request to the next handler of the router with the imported `wasi:http/handler` interface, or answers the request itself. Requests, responses and their bodies are WASI 0.3 resources and streams, so a plugin can change a body while it flows.

Traefik runs one instance of the plugin for each middleware. The instance serves all the requests of the middleware at the same time, as concurrent async calls of its `handle` export, which WASI 0.3 permits. Keep the state of a request out of global variables. A plugin that cannot serve concurrent requests can refuse them with the backpressure of the component model.

The plugin reads its routing configuration through the `wasi:config/store` interface. Each top-level option of the middleware configuration is one key. A string value is given as is, and any other value is JSON encoded.

The plugin targets the `traefik:plugin/middleware` world:

```wit
package traefik:plugin@0.1.0;

world middleware {
  include wasi:http/middleware@0.3.0;
  import wasi:config/store@0.2.0-rc.1;
}
```

A component built for the `wasm32-wasip2` target can also use the WASI 0.2 interfaces of its standard library. The `settings.envs` and `settings.mounts` plugin options give it environment variables and directories.

!!! warning "http-wasm plugins"
    Traefik does not run [http-wasm](https://http-wasm.io/) plugins anymore. A WASM plugin must be a component that implements the world above.

#### Key characteristics

- Multi-language support (Rust, C, Go, JavaScript, etc.)
- Compiled to a WebAssembly component
- Standard WASI 0.3 HTTP interfaces, with streamed bodies
- Strong security isolation
- Currently supports middleware only

## Build Your Own Plugins

Traefik users can create their own plugins and share them with the community using the [Plugin Catalog](https://plugins.traefik.io/). To learn more about Traefik plugin creation, please refer to the [developer documentation](https://plugins.traefik.io/create).
