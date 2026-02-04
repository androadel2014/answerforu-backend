// src/components/chat/ChatDock.jsx
import React, {
  useEffect,
  useMemo,
  useRef,
  useState,
  useCallback,
} from "react";
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
import { authHeaders } from "../profile/profilePage.parts";

const cn = (...a) => a.filter(Boolean).join(" ");

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const getToken = () => localStorage.getItem("token") || "";

function absMedia(u) {
  const s = String(u || "").trim();
  if (!s) return "";
  if (/^(data:|blob:|https?:\/\/)/i.test(s)) return s; // ✅ FIX
  return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
}

function firstNameOf(v) {
  const s = String(v || "").trim();
  if (!s) return "Someone";
  const parts = s.split(/\s+/).filter(Boolean);
  return parts[0] || s;
}

function getMeIdFromToken() {
  try {
    const t = getToken();
    if (!t) return null;
    const parts = t.split(".");
    if (parts.length < 2) return null;

    const payloadJson = atob(parts[1].replace(/-/g, "+").replace(/_/g, "/"));
    const payload = JSON.parse(payloadJson || "{}");

    const id =
      payload?.id ??
      payload?.userId ??
      payload?.user_id ??
      payload?.user?.id ??
      null;

    const n = Number(id);
    return Number.isFinite(n) && n > 0 ? n : null;
  } catch {
    return null;
  }
}

async function apiGet(url) {
  const res = await fetch(`${API_BASE}${url}`, {
    headers: { "Content-Type": "application/json", ...authHeaders() },
  });
  if (!res.ok) throw new Error(`GET ${url} failed`);
  return res.json();
}

async function apiPost(url, body) {
  const res = await fetch(`${API_BASE}${url}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
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

/* =========================
   ✅ profile link helper (non-sequential)
   - prefers username/handle/slug/public_id
   - fallback to numeric id (keeps old working)
========================= */
function pickUserKey(other) {
  const o = other || {};
  const s = String(
    o?.username ??
      o?.user_name ??
      o?.userName ??
      o?.handle ??
      o?.slug ??
      o?.public_id ??
      o?.publicId ??
      o?.uid ??
      "",
  ).trim();
  if (!s) return "";
  return s.startsWith("@") ? s.slice(1) : s;
}

function buildProfileHref(other) {
  const key = pickUserKey(other);
  if (key) return `/u/${encodeURIComponent(key)}`;

  const id = Number(
    other?.id ?? other?.user_id ?? other?.userId ?? other?.owner_id ?? 0,
  );
  if (Number.isFinite(id) && id > 0) return `/u/${id}`;
  return "";
}

function enrichThread(t) {
  const th = t || {};
  const other = th?.other || {};
  const href = buildProfileHref(other);
  return {
    ...th,
    other: { ...other, profile_href: href || other?.profile_href || "" },
  };
}

function Bubble({ mine, text, at, dir }) {
  return (
    <div className={cn("w-full flex", mine ? "justify-end" : "justify-start")}>
      <div
        className={cn(
          "max-w-[78%] rounded-2xl px-3 py-2 text-sm leading-relaxed",
          mine
            ? "bg-slate-900 text-white"
            : "bg-white text-slate-900 border border-slate-200",
        )}
        dir={dir}
      >
        <div className="whitespace-pre-wrap break-words">{text}</div>
        {!!at && (
          <div
            className={cn(
              "mt-1 text-[11px] opacity-70",
              mine ? "text-white/80" : "text-slate-500",
            )}
          >
            {at}
          </div>
        )}
      </div>
    </div>
  );
}

/* =========================
   Toast (avatar + first name)
   ✅ unread indicator OUTSIDE avatar (not on photo)
========================= */
function ChatToast({
  dockSide = "right",
  lang = "ar",
  toast,
  onOpen,
  onClose,
}) {
  if (!toast?.thread?.id) return null;

  const dir = lang === "ar" ? "rtl" : "ltr";
  const other = toast.thread?.other || {};
  const name = firstNameOf(other?.name || "User");
  const avatar = absMedia(other?.avatar_url);
  const text = String(toast.thread?.last_message || "").trim();

  const panel = (
    <div
      className="fixed z-[99995] bottom-[150px] md:bottom-[82px] pointer-events-none"
      style={dockSide === "right" ? { right: 18 } : { left: 18 }}
      dir={dir}
    >
      <div className="pointer-events-auto w-[320px] max-w-[86vw] rounded-2xl bg-white border border-slate-200 shadow-[0_20px_60px_rgba(0,0,0,0.18)] overflow-hidden">
        <button
          className="w-full text-left p-3 flex items-center gap-3 hover:bg-slate-50"
          onClick={() => onOpen?.(toast.thread)}
          title="Open chat"
        >
          <div className="h-10 w-10 rounded-full bg-slate-900 text-white flex items-center justify-center overflow-hidden shrink-0">
            {avatar ? (
              <img src={avatar} alt="" className="h-full w-full object-cover" />
            ) : (
              <span className="text-sm font-semibold">
                {name.slice(0, 1).toUpperCase()}
              </span>
            )}
          </div>

          <div className="min-w-0 flex-1">
            <div className="flex items-center justify-between gap-2">
              <div className="font-semibold text-slate-900 truncate">
                {name}
              </div>
              <div className="text-[11px] text-slate-400 shrink-0">
                {toast.atLabel || ""}
              </div>
            </div>
            <div className="text-sm text-slate-600 truncate">
              {text || "New message"}
            </div>
          </div>

          <div className="shrink-0 flex items-center">
            <span className="h-2.5 w-2.5 rounded-full bg-rose-600" />
          </div>
        </button>

        <div className="px-3 pb-3 -mt-1 flex justify-end">
          <button
            className="text-xs text-slate-500 hover:text-slate-700"
            onClick={onClose}
            type="button"
          >
            Dismiss
          </button>
        </div>
      </div>
    </div>
  );

  return createPortal(panel, document.body);
}

function ChatWindow({
  lang,
  thread,
  meId,
  onClose,
  onMinimize,
  onRead,
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
  const otherAvatar = absMedia(other?.avatar_url);

  const load = async () => {
    try {
      const data = await apiGet(`/api/chat/threads/${thread.id}/messages`);
      setMessages(Array.isArray(data?.items) ? data.items : []);
      setLoading(false);
      apiPost(`/api/chat/threads/${thread.id}/read`, {})
        .then(() => onRead?.(thread.id))
        .catch(() => {});
    } catch {
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
    } catch {
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
        <div
          className={cn(
            "h-12 flex items-center justify-between bg-slate-900 text-white",
            headerSidePadding,
          )}
          dir={dir}
        >
          <div className="flex items-center gap-2 min-w-0">
            <div className="h-8 w-8 rounded-full bg-white/10 flex items-center justify-center overflow-hidden">
              {otherAvatar ? (
                <img
                  src={otherAvatar}
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
                  : "bg-slate-900 text-white border-slate-900 hover:opacity-95",
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

function ChatInbox({ lang, open, onClose, onOpenThread, dockSide = "right" }) {
  const [q, setQ] = useState("");
  const [threads, setThreads] = useState([]);
  const [loading, setLoading] = useState(true);

  const dir = lang === "ar" ? "rtl" : "ltr";

  const load = async () => {
    try {
      const data = await apiGet(`/api/chat/threads`);
      const items = Array.isArray(data?.items) ? data.items : [];
      setThreads(items.map(enrichThread));
      setLoading(false);
    } catch {
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
        .includes(s),
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
          dockSide === "right" ? "right-4" : "left-4",
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
                const otherAvatar = absMedia(other?.avatar_url);
                const hasUnread = Number(t?.unread_count || 0) > 0;

                return (
                  <button
                    key={t.id}
                    className="w-full text-left p-3 hover:bg-slate-50 flex items-center gap-3"
                    onClick={() => onOpenThread(enrichThread(t))}
                  >
                    <div className="h-10 w-10 rounded-full bg-slate-900 text-white flex items-center justify-center overflow-hidden">
                      {otherAvatar ? (
                        <img
                          src={otherAvatar}
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

                        {hasUnread && (
                          <span className="h-2.5 w-2.5 rounded-full bg-rose-600 shrink-0" />
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

function MobileChatBody({ lang, thread, meId, onRead }) {
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
      apiPost(`/api/chat/threads/${thread.id}/read`, {})
        .then(() => onRead?.(thread.id))
        .catch(() => {});
    } catch {
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
    } catch {
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
                : "bg-slate-900 text-white border-slate-900 hover:opacity-95",
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

export default function ChatDock({
  lang = "ar",
  dockSide = "right",
  maxOpen = 3,
}) {
  const token = getToken();
  const [meId, setMeId] = useState(null);

  const [inboxOpen, setInboxOpen] = useState(false);
  const [openThreads, setOpenThreads] = useState([]);
  const [minimized, setMinimized] = useState([]);
  const [unreadTotal, setUnreadTotal] = useState(0);

  const [toast, setToast] = useState(null);
  const toastTimerRef = useRef(null);
  const prevMapRef = useRef(new Map()); // threadId -> { unread, last }

  const isAuthed = !!token;

  useEffect(() => {
    if (!isAuthed) {
      setMeId(null);
      return;
    }
    setMeId(getMeIdFromToken());
  }, [isAuthed, token]);

  const hideToast = useCallback(() => {
    setToast(null);
    if (toastTimerRef.current) {
      clearTimeout(toastTimerRef.current);
      toastTimerRef.current = null;
    }
  }, []);

  const showToast = useCallback(
    (thread) => {
      if (!thread?.id) return;

      const isThreadVisible =
        openThreads.some((x) => String(x.id) === String(thread.id)) &&
        !minimized.some((id) => String(id) === String(thread.id));

      if (inboxOpen) return;
      if (isThreadVisible) return;

      const atLabel = new Date().toLocaleTimeString([], {
        hour: "2-digit",
        minute: "2-digit",
      });

      setToast({ thread: enrichThread(thread), atLabel });

      if (toastTimerRef.current) clearTimeout(toastTimerRef.current);
      toastTimerRef.current = setTimeout(() => {
        setToast(null);
        toastTimerRef.current = null;
      }, 4500);
    },
    [inboxOpen, openThreads, minimized],
  );

  const mergeThreads = useCallback((freshItems) => {
    const list = Array.isArray(freshItems) ? freshItems : [];
    if (!list.length) return;

    setOpenThreads((prev) => {
      if (!prev.length) return prev;
      return prev.map((t) => {
        const found = list.find((x) => String(x.id) === String(t.id));
        return found ? enrichThread({ ...t, ...found }) : t;
      });
    });
  }, []);

  const markThreadRead = useCallback((id) => {
    setOpenThreads((prev) =>
      prev.map((t) =>
        String(t.id) === String(id) ? { ...t, unread_count: 0 } : t,
      ),
    );
  }, []);

  const refreshUnread = useCallback(async () => {
    if (!isAuthed) return;

    try {
      const d = await apiGet(`/api/chat/summary`);
      setUnreadTotal(Number(d?.unread_total || 0));
    } catch {}

    try {
      const t = await apiGet(`/api/chat/threads`);
      const items = Array.isArray(t?.items) ? t.items : [];
      const enrichedItems = items.map(enrichThread);
      mergeThreads(enrichedItems);

      const prevMap = prevMapRef.current || new Map();
      const nextMap = new Map();

      let candidate = null;

      for (const th of enrichedItems) {
        const id = String(th?.id || "");
        const unread = Number(th?.unread_count || 0) || 0;
        const last = String(th?.last_message || "");
        nextMap.set(id, { unread, last });

        const prev = prevMap.get(id);
        const prevUnread = Number(prev?.unread || 0) || 0;
        const prevLast = String(prev?.last || "");

        const becameMoreUnread = unread > prevUnread;
        const newMsgChanged = last && last !== prevLast;

        if (becameMoreUnread || (unread > 0 && newMsgChanged)) {
          if (!candidate) candidate = th;
        }
      }

      prevMapRef.current = nextMap;

      if (candidate && Number(candidate?.unread_count || 0) > 0) {
        showToast(candidate);
      }
    } catch {}
  }, [isAuthed, mergeThreads, showToast]);

  useInterval(() => refreshUnread(), isAuthed ? 10000 : null);

  useEffect(() => {
    refreshUnread();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAuthed]);

  const openThread = useCallback(
    (t) => {
      const th = enrichThread(t);
      if (!th?.id) return;

      hideToast();
      setInboxOpen(false);

      setOpenThreads((prev) => {
        const exists = prev.some((x) => String(x.id) === String(th.id));
        if (exists) {
          return prev.map((x) =>
            String(x.id) === String(th.id) ? { ...x, ...th } : x,
          );
        }
        return [th, ...prev].slice(0, maxOpen);
      });

      setMinimized((prev) => prev.filter((id) => String(id) !== String(th.id)));
    },
    [maxOpen, hideToast],
  );

  useEffect(() => {
    const onOpen = (ev) => {
      const thread = ev?.detail?.thread;
      if (thread?.id) openThread(thread);
      else setInboxOpen(true);
    };

    window.addEventListener("a4u:chat-open", onOpen);
    return () => window.removeEventListener("a4u:chat-open", onOpen);
  }, [openThread]);

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

  const visibleThreads = openThreads.filter(
    (t) => !minimized.some((id) => String(id) === String(t.id)),
  );

  const baseOffset = 18;
  const gap = 10;
  const winWidth = 320;

  const button = (
    <button
      className={cn(
        "fixed z-[99970] bottom-[92px] md:bottom-[22px] h-12 w-12 rounded-2xl shadow-lg flex items-center justify-center",
        "bg-slate-900 text-white hover:opacity-95",
      )}
      style={dockSide === "right" ? { right: 18 } : { left: 18 }}
      onClick={() => {
        hideToast();
        setInboxOpen(true);
      }}
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
          dockSide === "right" ? "right-[78px]" : "left-[78px]",
        )}
      >
        {minimized.slice(0, 6).map((id) => {
          const t = openThreads.find((x) => String(x.id) === String(id));
          const name = t?.other?.name || "Chat";
          const hasUnread = Number(t?.unread_count || 0) > 0;
          const otherAvatar = absMedia(t?.other?.avatar_url);

          return (
            <button
              key={id}
              onClick={() => {
                hideToast();
                restoreThread(id);
              }}
              className="relative h-12 max-w-[180px] px-3 rounded-2xl bg-white border border-slate-200 shadow-sm hover:bg-slate-50 flex items-center gap-2"
              title={name}
            >
              <div className="h-8 w-8 rounded-full bg-slate-900 text-white flex items-center justify-center overflow-hidden text-xs font-semibold">
                {otherAvatar ? (
                  <img
                    src={otherAvatar}
                    alt=""
                    className="h-full w-full object-cover"
                  />
                ) : (
                  name.slice(0, 1).toUpperCase()
                )}
              </div>

              <div className="text-sm font-semibold text-slate-900 truncate">
                {firstNameOf(name)}
              </div>

              {hasUnread && (
                <span className="ml-auto h-2.5 w-2.5 rounded-full bg-rose-600" />
              )}
            </button>
          );
        })}
      </div>
    );

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
          onRead={markThreadRead}
          dockSide={dockSide}
          width={winWidth}
        />
      </div>
    );
  });

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
        <MobileChatBody
          lang={lang}
          thread={mobileActive}
          meId={meId}
          onRead={markThreadRead}
        />
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
        onOpenThread={(t) => {
          openThread(t);
          apiPost(`/api/chat/threads/${t.id}/read`, {})
            .then(() => markThreadRead(t.id))
            .catch(() => {});
        }}
        dockSide={dockSide}
      />
      {mobileDrawer}

      <ChatToast
        dockSide={dockSide}
        lang={lang}
        toast={toast}
        onOpen={(t) => openThread(t)}
        onClose={hideToast}
      />
    </>
  );
}
