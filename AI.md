# AI RULES – PLANX SDK (v4)

You are working inside Planx SDK.

This SDK owns ALL runtime semantics.

---

## SDK HARD RULES

You MUST NOT:

1. Move runtime logic into Engine
2. Move runtime logic into Plugins
3. Expose gRPC, sessions, or flow control to plugin developers
4. Allow plugins to control concurrency or backpressure
5. Parse payload data (opaque bytes only)
6. Introduce single-record APIs
7. Leak session, window, or flow control to SPI
8. Import planx-engine

YOU MUST:

1. Keep SPI minimal and synchronous
2. Hide runtime complexity inside internal/
3. Treat plugins as user code, not system code

---

## SDK IS THE ONLY PLACE FOR

- gRPC servers
- Session lifecycle
- Flow control (window blocking)
- Batch packing/unpacking
- Panic recovery
- Observability hooks

---

If any change violates these rules:
STOP and ASK.
