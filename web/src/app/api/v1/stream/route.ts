/**
 * Phase 4 dedicated SSE proxy.
 * Do not use the buffered [...path] route (response.text() + 20s timeout).
 * SharedWorker / MarketCache / terminal UI are out of scope.
 */
export const runtime = "nodejs";
export const dynamic = "force-dynamic";

const UPSTREAM =
  process.env.NSE_API_UPSTREAM ?? "http://127.0.0.1:8080";

export async function GET(request: Request) {
  const url = new URL(request.url);
  const target = `${UPSTREAM}/api/v1/stream${url.search}`;
  const headers = new Headers();
  const lastEventId =
    request.headers.get("last-event-id") ?? request.headers.get("Last-Event-ID");
  if (lastEventId) {
    headers.set("Last-Event-ID", lastEventId);
  }
  headers.set("Accept", "text/event-stream");

  const upstream = await fetch(target, {
    method: "GET",
    headers,
    cache: "no-store",
    // Long-lived: no finite proxy timeout. Abort follows the browser client.
    signal: request.signal,
  });

  if (!upstream.ok || !upstream.body) {
    return new Response(await upstream.text(), {
      status: upstream.status,
      headers: {
        "content-type": upstream.headers.get("content-type") ?? "application/json",
        "retry-after": upstream.headers.get("retry-after") ?? "",
      },
    });
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "content-type": "text/event-stream",
      "cache-control": "no-store",
      "x-accel-buffering": "no",
      connection: "keep-alive",
    },
  });
}
