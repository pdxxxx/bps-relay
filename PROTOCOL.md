# Protocol 1

All frames are UTF-8 JSON WebSocket text messages. Binary HTTP body bytes are
base64 encoded in bounded data frames. WebSocket compression is not used.
The server sends `{type:"hello",status:1,bytes:262144}` after upgrade. No request
is sent until this handshake is validated. IDs increase monotonically within a
connection; the same numeric ID on a different connection has independent state.

1. Client `start`: id, method, exact URL, allowed headers, declared byte length.
2. Server `ready`: initial upload credit, 262144 bytes.
3. Client `upload`: at most 32768 decoded bytes. Server writes the temporary
   request file and returns `upload_credit` with the consumed byte count.
4. Client `uploaded`: body is complete. The server verifies declared length and
   starts the original HTTPS request. The body is non-replayable to the HTTP client.
5. Server `head`: status and supported response headers. Redirects are not followed.
6. Server `data`: response bytes. The initial response window is 262144 bytes.
   Client `credit` returns consumed bytes; the server pauses when credit is zero.
7. Server `end`: upstream EOF. `error` fails only the named request with a fixed
   code and no upstream payload details. Client `cancel` cancels only its ID.

`ping`/`pong` and native WebSocket ping/pong keep idle connections alive. They do
not extend the caller's own explicit timeout. Disconnection aborts all requests
on that connection; the protocol does not claim resumability or replay safety.

The frame limit (128 KiB encoded) is a transport framing rule, not an aggregate
body-size limit. Header metadata is bounded independently. Upstream active
requests and client connections have no configured count limit.
