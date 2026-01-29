// src/components/chat/ChatDock.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import {
  MessageCircle,
  X,
  Minus,
  Search,
  ArrowLeft,
  ArrowRight,
  SendHorizontal,
} from "lucide-react";

const cn = (...a) => a.filter(Boolean).join(" ");

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const getToken = () => localStorage.getItem("token") || "";

async function apiGet(url) {
  const res = await fetch(`${API_BASE}${url}`, {
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getToken()}`,
    },
  });
  if (!res.ok) throw new Error(`GET ${url} failed`);
  return res.json();
}

async function apiPost(url, body) {
  const res = await fetch(`${API_BASE}${url}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${getToken()}`,
    },
    body: JSON.stringify(body || {}),
  });
  if (!res.ok) throw new Error(`POST ${url} failed`);
  return res.json();
}

function useInterval(fn, delay) {
  const ref = useRef(fn);
  useEffect(() => {
    ref.current = fn;
  }, [fn]);
  useEffect(() => {
    if (delay == null) return;
    const id = setInterval(() => ref.current(), delay);
    return () => clearInterval(id);
  }, [delay]);
}

function Bubble({ mine, text, at, dir }) {
  return (
    <div className={cn("w-full flex", mine ? "justify-end" : "justify-start")}>
      <div
        className={cn(
          "max-w-[78%] rounded-2xl px-3 py-2 text-sm leading-relaxed",
          mine
            ? "bg-slate-900 text-white"
            : "bg-white text-slate-900 border border-slate-200"
        )}
        dir={dir}
      >
        <div className="whitespace-pre-wrap break-words">{text}</div>
        {!!at && (
          <div
            className={cn(
              "mt-1 text-[11px] opacity-70",
              mine ? "text-white/80" : "text-slate-500"
            )}
          >
            {at}
          </div>
        )}
      </div>
    </div>
  );
}

function ChatWindow({
  lang,
  thread,
  meId,
  onClose,
  onMinimize,
  dockSide = "right",
  width = 320,
}) {
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const bodyRef = useRef(null);

  const other = thread?.other || {};
  const dir = lang === "ar" ? "rtl" : "ltr";

  const load = async () => {
    try {
      const data = await apiGet(`/api/chat/threads/${thread.id}/messages`);
      setMessages(Array.isArray(data?.items) ? data.items : []);
      setLoading(false);
      apiPost(`/api/chat/threads/${thread.id}/read`, {}).catch(() => {});
    } catch (e) {
      setLoading(false);
    }
  };

  useEffect(() => {
    setLoading(true);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [thread?.id]);

  useEffect(() => {
    if (!bodyRef.current) return;
    bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [messages?.length]);

  const send = async () => {
    const text = String(draft || "").trim();
    if (!text || sending) return;
    setSending(true);
    setDraft("");
    try {
      const r = await apiPost(`/api/chat/threads/${thread.id}/messages`, {
        body: text,
      });
      const item = r?.item;
      if (item) setMessages((prev) => [...prev, item]);
      else await load();
    } catch (e) {
      // restore draft if failed
      setDraft(text);
    } finally {
      setSending(false);
      setTimeout(() => {
        if (!bodyRef.current) return;
        bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
      }, 50);
    }
  };

  const headerSidePadding = dockSide === "right" ? "pl-3 pr-2" : "pr-3 pl-2";

  return (
    <div
      className="fixed bottom-[84px] z-[99990] hidden md:block"
      style={dockSide === "right" ? { right: 18, width } : { left: 18, width }}
    >
      <div className="rounded-2xl overflow-hidden bg-white border border-slate-200 shadow-[0_20px_60px_rgba(0,0,0,0.18)]">
        {/* header */}
        <div
          className={cn(
            "h-12 flex items-center justify-between bg-slate-900 text-white",
            headerSidePadding
          )}
          dir={dir}
        >
          <div className="flex items-center gap-2 min-w-0">
            <div className="h-8 w-8 rounded-full bg-white/10 flex items-center justify-center overflow-hidden">
              {other?.avatar_url ? (
                <img
                  src={other.avatar_url}
                  alt=""
                  className="h-full w-full object-cover"
                />
              ) : (
                <span className="text-sm font-semibold">
                  {(other?.name || "U").slice(0, 1).toUpperCase()}
                </span>
              )}
            </div>
            <div className="min-w-0">
              <div className="text-sm font-semibold truncate">
                {other?.name || "Chat"}
              </div>
              {!!thread?.context_label && (
                <div className="text-[11px] text-white/70 truncate">
                  {thread.context_label}
                </div>
              )}
            </div>
          </div>

          <div className="flex items-center gap-1">
            <button
              className="h-8 w-8 rounded-full hover:bg-white/10 flex items-center justify-center"
              onClick={onMinimize}
              title="Minimize"
            >
              <Minus className="h-4 w-4" />
            </button>
            <button
              className="h-8 w-8 rounded-full hover:bg-white/10 flex items-center justify-center"
              onClick={onClose}
              title="Close"
            >
              <X className="h-4 w-4" />
            </button>
          </div>
        </div>

        {/* body */}
        <div
          ref={bodyRef}
          className="h-[340px] overflow-y-auto px-3 py-3 bg-slate-50"
        >
          {loading ? (
            <div className="text-sm text-slate-500">Loading…</div>
          ) : messages.length === 0 ? (
            <div className="text-sm text-slate-500">No messages yet.</div>
          ) : (
            <div className="flex flex-col gap-2">
              {messages.map((m) => (
                <Bubble
                  key={m.id}
                  mine={String(m.sender_id) === String(meId)}
                  text={m.body}
                  at={m.created_at_label || ""}
                  dir={dir}
                />
              ))}
            </div>
          )}
        </div>

        {/* footer */}
        <div className="p-2 border-t border-slate-200 bg-white">
          <div className="flex items-end gap-2">
            <textarea
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              placeholder="Type a message…"
              className="flex-1 resize-none rounded-2xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-900/15"
              rows={1}
              onKeyDown={(e) => {
                if (e.key === "Enter" && !e.shiftKey) {
                  e.preventDefault();
                  send();
                }
              }}
              dir={dir}
            />
            <button
              onClick={send}
              disabled={sending || !String(draft || "").trim()}
              className={cn(
                "h-10 w-10 rounded-2xl flex items-center justify-center border",
                sending || !String(draft || "").trim()
                  ? "bg-slate-100 text-slate-400 border-slate-200"
                  : "bg-slate-900 text-white border-slate-900 hover:opacity-95"
              )}
              title="Send"
            >
              <SendHorizontal className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function ChatInbox({
  lang,
  open,
  onClose,
  onOpenThread,
  meId,
  dockSide = "right",
}) {
  const [q, setQ] = useState("");
  const [threads, setThreads] = useState([]);
  const [loading, setLoading] = useState(true);

  const dir = lang === "ar" ? "rtl" : "ltr";

  const load = async () => {
    try {
      const data = await apiGet(`/api/chat/threads`);
      setThreads(Array.isArray(data?.items) ? data.items : []);
      setLoading(false);
    } catch (e) {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!open) return;
    setLoading(true);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const filtered = useMemo(() => {
    const s = String(q || "")
      .trim()
      .toLowerCase();
    if (!s) return threads;
    return threads.filter((t) =>
      String(t?.other?.name || "")
        .toLowerCase()
        .includes(s)
    );
  }, [threads, q]);

  if (!open) return null;

  const panel = (
    <div
      className="fixed inset-0 z-[99980]"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        className={cn(
          "fixed bottom-[84px] md:bottom-[96px] w-[360px] max-w-[92vw] rounded-2xl overflow-hidden bg-white border border-slate-200 shadow-[0_20px_60px_rgba(0,0,0,0.18)]",
          dockSide === "right" ? "right-4" : "left-4"
        )}
      >
        <div
          className="h-12 bg-white border-b border-slate-200 flex items-center justify-between px-3"
          dir={dir}
        >
          <div className="font-semibold text-slate-900">Messages</div>
          <button
            className="h-8 w-8 rounded-full hover:bg-slate-100 flex items-center justify-center"
            onClick={onClose}
            title="Close"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="p-3 border-b border-slate-200">
          <div className="flex items-center gap-2 rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2">
            <Search className="h-4 w-4 text-slate-500" />
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              className="flex-1 bg-transparent outline-none text-sm"
              placeholder="Search…"
              dir={dir}
            />
          </div>
        </div>

        <div className="max-h-[420px] overflow-y-auto">
          {loading ? (
            <div className="p-3 text-sm text-slate-500">Loading…</div>
          ) : filtered.length === 0 ? (
            <div className="p-3 text-sm text-slate-500">No conversations.</div>
          ) : (
            <div className="divide-y divide-slate-100">
              {filtered.map((t) => {
                const other = t?.other || {};
                return (
                  <button
                    key={t.id}
                    className="w-full text-left p-3 hover:bg-slate-50 flex items-center gap-3"
                    onClick={() => onOpenThread(t)}
                  >
                    <div className="h-10 w-10 rounded-full bg-slate-900 text-white flex items-center justify-center overflow-hidden">
                      {other?.avatar_url ? (
                        <img
                          src={other.avatar_url}
                          alt=""
                          className="h-full w-full object-cover"
                        />
                      ) : (
                        <span className="text-sm font-semibold">
                          {(other?.name || "U").slice(0, 1).toUpperCase()}
                        </span>
                      )}
                    </div>

                    <div className="min-w-0 flex-1">
                      <div className="flex items-center justify-between gap-2">
                        <div className="font-semibold text-slate-900 truncate">
                          {other?.name || "User"}
                        </div>
                        {!!t.unread_count && (
                          <div className="min-w-[22px] h-[22px] px-2 rounded-full bg-rose-600 text-white text-xs flex items-center justify-center">
                            {t.unread_count}
                          </div>
                        )}
                      </div>
                      <div className="text-sm text-slate-600 truncate">
                        {t.last_message || " "}
                      </div>
                      {!!t.context_label && (
                        <div className="text-[11px] text-slate-400 truncate">
                          {t.context_label}
                        </div>
                      )}
                    </div>
                  </button>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );

  return createPortal(panel, document.body);
}

export default function ChatDock({
  lang = "ar",
  dockSide = "right",
  maxOpen = 3,
}) {
  const token = getToken();
  const [meId, setMeId] = useState(null);

  const [inboxOpen, setInboxOpen] = useState(false);
  const [openThreads, setOpenThreads] = useState([]); // [{id, other, ...}]
  const [minimized, setMinimized] = useState([]); // thread ids
  const [unreadTotal, setUnreadTotal] = useState(0);

  const isAuthed = !!token;

  // resolve me id (lightweight)
  useEffect(() => {
    if (!isAuthed) {
      setMeId(null);
      return;
    }
    apiGet(`/api/users/me`)
      .then((d) => setMeId(d?.user?.id || d?.id || null))
      .catch(() => setMeId(null));
  }, [isAuthed]);

  const refreshUnread = async () => {
    if (!isAuthed) return;
    try {
      const d = await apiGet(`/api/chat/summary`);
      setUnreadTotal(Number(d?.unread_total || 0));
    } catch (e) {}
  };

  // poll unread badge
  useInterval(
    () => {
      refreshUnread();
    },
    isAuthed ? 10000 : null
  );

  useEffect(() => {
    refreshUnread();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAuthed]);

  const openThread = (t) => {
    if (!t?.id) return;

    setInboxOpen(false);

    setOpenThreads((prev) => {
      const exists = prev.some((x) => String(x.id) === String(t.id));
      if (exists) return prev;

      const next = [t, ...prev].slice(0, maxOpen);
      return next;
    });

    setMinimized((prev) => prev.filter((id) => String(id) !== String(t.id)));
  };

  const closeThread = (id) => {
    setOpenThreads((prev) => prev.filter((t) => String(t.id) !== String(id)));
    setMinimized((prev) => prev.filter((x) => String(x) !== String(id)));
  };

  const minimizeThread = (id) => {
    setMinimized((prev) => {
      if (prev.some((x) => String(x) === String(id))) return prev;
      return [...prev, id];
    });
  };

  const restoreThread = (id) => {
    setMinimized((prev) => prev.filter((x) => String(x) !== String(id)));
  };

  const dockDir = lang === "ar" ? "rtl" : "ltr";

  // windows order: keep most recent left->right for right dock, and vice versa
  const visibleThreads = openThreads.filter(
    (t) => !minimized.some((id) => String(id) === String(t.id))
  );

  // position stacking
  const baseOffset = 18;
  const gap = 10;
  const winWidth = 320;

  const button = (
    <button
      className={cn(
        "fixed z-[99970] bottom-[92px] md:bottom-[22px] h-12 w-12 rounded-2xl shadow-lg flex items-center justify-center",
        "bg-slate-900 text-white hover:opacity-95"
      )}
      style={dockSide === "right" ? { right: 18 } : { left: 18 }}
      onClick={() => setInboxOpen(true)}
      title="Messages"
    >
      <MessageCircle className="h-5 w-5" />
      {!!unreadTotal && (
        <span className="absolute -top-1 -right-1 min-w-[22px] h-[22px] px-2 rounded-full bg-rose-600 text-white text-xs flex items-center justify-center">
          {unreadTotal > 99 ? "99+" : unreadTotal}
        </span>
      )}
    </button>
  );

  const minimizedBar =
    minimized.length === 0 ? null : (
      <div
        className={cn(
          "fixed z-[99975] bottom-[92px] md:bottom-[22px] h-12 flex items-center gap-2",
          dockSide === "right" ? "right-[78px]" : "left-[78px]"
        )}
        dir={dockDir}
      >
        {minimized.slice(0, 6).map((id) => {
          const t = openThreads.find((x) => String(x.id) === String(id));
          const name = t?.other?.name || "Chat";
          return (
            <button
              key={id}
              onClick={() => restoreThread(id)}
              className="h-12 max-w-[180px] px-3 rounded-2xl bg-white border border-slate-200 shadow-sm hover:bg-slate-50 flex items-center gap-2"
              title={name}
            >
              <div className="h-8 w-8 rounded-full bg-slate-900 text-white flex items-center justify-center text-xs font-semibold">
                {name.slice(0, 1).toUpperCase()}
              </div>
              <div className="text-sm font-semibold text-slate-900 truncate">
                {name}
              </div>
            </button>
          );
        })}
      </div>
    );

  // render chat windows (desktop)
  const windows = visibleThreads.map((t, idx) => {
    const offset = idx * (winWidth + gap);
    const style =
      dockSide === "right"
        ? { right: baseOffset + offset, width: winWidth }
        : { left: baseOffset + offset, width: winWidth };

    return (
      <div
        key={t.id}
        className="hidden md:block"
        style={{ position: "fixed", bottom: 0, zIndex: 99990, ...style }}
      >
        <ChatWindow
          lang={lang}
          thread={t}
          meId={meId}
          onClose={() => closeThread(t.id)}
          onMinimize={() => minimizeThread(t.id)}
          dockSide={dockSide}
          width={winWidth}
        />
      </div>
    );
  });

  // mobile: open only latest one as fullscreen drawer
  const mobileActive = visibleThreads[0] || null;

  const mobileDrawer = mobileActive ? (
    <div className="fixed inset-0 z-[99990] md:hidden bg-white">
      <div className="h-14 px-3 border-b border-slate-200 flex items-center justify-between bg-white">
        <button
          className="h-10 w-10 rounded-2xl hover:bg-slate-100 flex items-center justify-center"
          onClick={() => closeThread(mobileActive.id)}
          title="Back"
        >
          {dockSide === "right" ? (
            <ArrowRight className="h-5 w-5" />
          ) : (
            <ArrowLeft className="h-5 w-5" />
          )}
        </button>
        <div className="font-semibold truncate">
          {mobileActive?.other?.name || "Chat"}
        </div>
        <button
          className="h-10 w-10 rounded-2xl hover:bg-slate-100 flex items-center justify-center"
          onClick={() => minimizeThread(mobileActive.id)}
          title="Minimize"
        >
          <Minus className="h-5 w-5" />
        </button>
      </div>

      <div className="h-[calc(100vh-56px)]">
        {/* reuse ChatWindow layout but simplified */}
        <MobileChatBody lang={lang} thread={mobileActive} meId={meId} />
      </div>
    </div>
  ) : null;

  if (!isAuthed) return null;

  return (
    <>
      {button}
      {minimizedBar}
      {windows}
      <ChatInbox
        lang={lang}
        open={inboxOpen}
        onClose={() => setInboxOpen(false)}
        onOpenThread={openThread}
        meId={meId}
        dockSide={dockSide}
      />
      {mobileDrawer}
    </>
  );
}

function MobileChatBody({ lang, thread, meId }) {
  const [messages, setMessages] = useState([]);
  const [loading, setLoading] = useState(true);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const bodyRef = useRef(null);

  const dir = lang === "ar" ? "rtl" : "ltr";

  const load = async () => {
    try {
      const data = await apiGet(`/api/chat/threads/${thread.id}/messages`);
      setMessages(Array.isArray(data?.items) ? data.items : []);
      setLoading(false);
      apiPost(`/api/chat/threads/${thread.id}/read`, {}).catch(() => {});
    } catch (e) {
      setLoading(false);
    }
  };

  useEffect(() => {
    setLoading(true);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [thread?.id]);

  useEffect(() => {
    if (!bodyRef.current) return;
    bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [messages?.length]);

  const send = async () => {
    const text = String(draft || "").trim();
    if (!text || sending) return;
    setSending(true);
    setDraft("");
    try {
      const r = await apiPost(`/api/chat/threads/${thread.id}/messages`, {
        body: text,
      });
      const item = r?.item;
      if (item) setMessages((prev) => [...prev, item]);
      else await load();
    } catch (e) {
      setDraft(text);
    } finally {
      setSending(false);
      setTimeout(() => {
        if (!bodyRef.current) return;
        bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
      }, 50);
    }
  };

  return (
    <div className="h-full flex flex-col">
      <div
        ref={bodyRef}
        className="flex-1 overflow-y-auto px-3 py-3 bg-slate-50"
      >
        {loading ? (
          <div className="text-sm text-slate-500">Loading…</div>
        ) : messages.length === 0 ? (
          <div className="text-sm text-slate-500">No messages yet.</div>
        ) : (
          <div className="flex flex-col gap-2">
            {messages.map((m) => (
              <Bubble
                key={m.id}
                mine={String(m.sender_id) === String(meId)}
                text={m.body}
                at={m.created_at_label || ""}
                dir={dir}
              />
            ))}
          </div>
        )}
      </div>

      <div className="p-2 border-t border-slate-200 bg-white">
        <div className="flex items-end gap-2">
          <textarea
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            placeholder="Type a message…"
            className="flex-1 resize-none rounded-2xl border border-slate-200 bg-white px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-900/15"
            rows={1}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                send();
              }
            }}
            dir={dir}
          />
          <button
            onClick={send}
            disabled={sending || !String(draft || "").trim()}
            className={cn(
              "h-10 w-10 rounded-2xl flex items-center justify-center border",
              sending || !String(draft || "").trim()
                ? "bg-slate-100 text-slate-400 border-slate-200"
                : "bg-slate-900 text-white border-slate-900 hover:opacity-95"
            )}
            title="Send"
          >
            <SendHorizontal className="h-4 w-4" />
          </button>
        </div>
      </div>
    </div>
  );
}
