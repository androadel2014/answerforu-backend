// src/components/carry/CarryListingDetails.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import { useLocation, useNavigate, useParams } from "react-router-dom";
import toast from "react-hot-toast";
import {
  ArrowLeft,
  CheckCircle2,
  XCircle,
  Lock,
  MessageCircle,
  Pencil,
  Trash2,
  RefreshCw,
  User,
  Calendar,
  Weight,
  DollarSign,
  MapPin,
  SendHorizontal,
} from "lucide-react";

import { authHeaders } from "../../lib/apiHelpers";
import { toastConfirm } from "../../lib/notify";

const API_BASE =
  import.meta.env.VITE_API_BASE_URL ||
  import.meta.env.VITE_API_BASE ||
  "http://localhost:5000";

const cn = (...a) => a.filter(Boolean).join(" ");

function fmtDT(v) {
  if (!v) return "—";
  return String(v).replace("T", " ").replace("Z", "");
}

function pillBase(color) {
  return cn(
    "inline-flex items-center gap-1 rounded-full px-3 py-1 text-xs font-semibold",
    color
  );
}

function StatusPill({ status }) {
  const s = String(status || "open").toLowerCase();
  const cls =
    s === "open"
      ? "bg-emerald-50 text-emerald-700 border border-emerald-100"
      : s === "matched" || s === "accepted"
      ? "bg-indigo-50 text-indigo-700 border border-indigo-100"
      : "bg-slate-50 text-slate-700 border border-slate-100";
  return <span className={pillBase(cls)}>{s}</span>;
}

function RolePill({ role }) {
  const r = String(role || "").toLowerCase();
  const cls =
    r === "traveler"
      ? "bg-violet-50 text-violet-700 border border-violet-100"
      : "bg-amber-50 text-amber-800 border border-amber-100";
  return <span className={pillBase(cls)}>{r || "—"}</span>;
}

function Card({ title, icon: Icon, children, right }) {
  return (
    <div className="rounded-2xl border bg-white p-4 shadow-sm mb-3">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          {Icon ? <Icon className="h-4 w-4 opacity-70" /> : null}
          <div className="text-sm font-semibold">{title}</div>
        </div>
        {right}
      </div>
      {children}
    </div>
  );
}

function PrimaryButton({ children, className, ...props }) {
  return (
    <button
      {...props}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-sm font-semibold",
        "bg-slate-900 text-white hover:bg-slate-800 active:bg-slate-950",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        className
      )}
    >
      {children}
    </button>
  );
}
function SecondaryButton({ children, className, ...props }) {
  return (
    <button
      {...props}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-sm font-semibold",
        "border bg-white hover:bg-slate-50 active:bg-slate-100",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        className
      )}
    >
      {children}
    </button>
  );
}
function DangerButton({ children, className, ...props }) {
  return (
    <button
      {...props}
      className={cn(
        "inline-flex items-center justify-center gap-2 rounded-xl px-4 py-2 text-sm font-semibold",
        "border border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100 active:bg-rose-200",
        "disabled:opacity-50 disabled:cursor-not-allowed",
        className
      )}
    >
      {children}
    </button>
  );
}

function Modal({ open, title, children, onClose }) {
  if (!open) return null;
  return (
    <div className="fixed inset-0 z-[80] flex items-center justify-center p-4">
      <div
        className="absolute inset-0 bg-black/40"
        onClick={onClose}
        aria-hidden="true"
      />
      <div className="relative w-full max-w-lg rounded-2xl border bg-white shadow-xl">
        <div className="flex items-center justify-between border-b px-4 py-3">
          <div className="text-sm font-semibold">{title}</div>
          <button
            className="rounded-lg px-2 py-1 text-sm hover:bg-slate-100"
            onClick={onClose}
            type="button"
          >
            ✕
          </button>
        </div>
        <div className="p-4">{children}</div>
      </div>
    </div>
  );
}

/* =========================
   Local fetch
========================= */
async function callJSON(url, opts = {}) {
  const res = await fetch(url, opts);
  const ct = res.headers.get("content-type") || "";
  let data = null;
  try {
    data = ct.includes("application/json")
      ? await res.json()
      : await res.text();
  } catch {
    data = null;
  }
  return { ok: res.ok, status: res.status, data };
}

async function callFirstOk(calls) {
  let last = null;
  for (const c of calls) {
    try {
      const r = await callJSON(c.url, c.opts);
      last = r;
      if (r.ok) return r;
      if (r.status === 404) continue;
      return r;
    } catch (e) {
      last = { ok: false, status: 0, data: { error: String(e?.message || e) } };
    }
  }
  return last || { ok: false, status: 404, data: { error: "Not found" } };
}

/* =========================
   Auth helpers + Login button
========================= */
function getTokenLocal() {
  try {
    const t =
      localStorage.getItem("token") ||
      localStorage.getItem("access_token") ||
      localStorage.getItem("auth_token") ||
      localStorage.getItem("jwt") ||
      "";
    return String(t || "").trim();
  } catch {
    return "";
  }
}
function isAuthed() {
  return !!getTokenLocal();
}
function getMyUserIdLocal() {
  try {
    const candidates = ["user", "currentUser", "profile", "me"];
    for (const k of candidates) {
      const raw = localStorage.getItem(k);
      if (!raw) continue;
      const obj = JSON.parse(raw);
      const id = obj?.id || obj?.user?.id || obj?.me?.id;
      if (id != null) return Number(id);
    }
  } catch {}
  return null;
}

function LoginLinkButton({ onClick, className }) {
  return (
    <button
      onClick={onClick}
      className={cn(
        "inline-flex items-center gap-2 rounded-xl px-3 py-2 text-sm font-semibold",
        "border bg-white hover:bg-slate-50 active:bg-slate-100",
        className
      )}
      type="button"
    >
      <Lock className="h-4 w-4" />
      Login
    </button>
  );
}

// ✅ request status normalization
function normReqStatus(v) {
  const s = String(v || "")
    .toLowerCase()
    .trim();
  if (!s) return null;
  if (s === "sent") return "pending";
  if (s === "approved" || s === "matched") return "accepted";
  return s; // pending | accepted | rejected | canceled ...
}

export default function CarryListingDetails() {
  const nav = useNavigate();
  const loc = useLocation();
  const { id } = useParams();
  const listingId = Number(id);

  const [loading, setLoading] = useState(true);
  const [item, setItem] = useState(null);

  const [meta, setMeta] = useState({
    requests_count: 0,
    messages: [],
    avg_rating: 0,
    reviews_count: 0,
    can_chat: false,
    is_owner: false,
    my_request_status: null,
    my_request_id: null,
    my_request: null,
  });

  const [reqLoading, setReqLoading] = useState(false);
  const [requests, setRequests] = useState([]);

  const [myReqRow, setMyReqRow] = useState(null);

  const [requestModalOpen, setRequestModalOpen] = useState(false);
  const [noteDraft, setNoteDraft] = useState("");

  const [offerStep, setOfferStep] = useState(1);
  const [priceDraft, setPriceDraft] = useState("");
  const [currencyDraft, setCurrencyDraft] = useState("USD");

  const [msgDraft, setMsgDraft] = useState("");
  const [chatLoading, setChatLoading] = useState(false);

  const authed = isAuthed();
  const myUserId = getMyUserIdLocal();
  const isOwner = !!meta.is_owner;

  const myStatus = normReqStatus(meta.my_request_status);

  // ✅ ONLY allow chat after accepted
  const chatUnlocked = !!meta.can_chat && (isOwner || myStatus === "accepted");

  const chatEndRef = useRef(null);

  function scrollChatToBottom(behavior = "smooth") {
    try {
      chatEndRef.current?.scrollIntoView({ behavior, block: "end" });
    } catch {}
  }

  function goLogin() {
    nav("/auth", { state: { next: loc.pathname } });
  }

  function resetOfferModal() {
    setOfferStep(1);
    setPriceDraft("");
    setCurrencyDraft("USD");
    setNoteDraft("");
  }

  function buildOfferNote(noteOnly) {
    const p = String(priceDraft || "").trim();
    const c = String(currencyDraft || "USD").trim();
    const n = String(noteOnly || "").trim();
    const priceLine = p ? `OFFER_PRICE: ${c} ${p}` : "";
    return [priceLine, n].filter(Boolean).join("\n\n");
  }

  async function loadDetails() {
    setLoading(true);
    try {
      const r = await callJSON(`${API_BASE}/api/carry/listings/${listingId}`, {
        headers: { ...authHeaders() },
      });
      if (!r?.ok) throw new Error("bad");

      const data = r.data || {};
      setItem(data.item);

      setMeta({
        requests_count: Number(data.requests_count || 0),
        messages: data.messages || [],
        avg_rating: Number(data.avg_rating || 0),
        reviews_count: Number(data.reviews_count || 0),
        can_chat: !!data.can_chat,
        is_owner: !!data.is_owner,
        my_request_status: data.my_request_status || null,
        my_request_id: data.my_request_id || null,
        my_request: data.my_request || null,
      });

      if (data.my_request) setMyReqRow(data.my_request);
      else if (data.my_request_id)
        setMyReqRow((x) => (x?.id ? x : { id: data.my_request_id }));
      else setMyReqRow(null);
    } catch {
      toast.error("Failed to load listing");
    } finally {
      setLoading(false);
    }
  }

  async function loadOwnerRequests() {
    if (!listingId) return;
    if (!isOwner) return;

    setReqLoading(true);
    try {
      const r = await callJSON(
        `${API_BASE}/api/carry/listings/${listingId}/requests`,
        { headers: { ...authHeaders() } }
      );
      if (!r?.ok) throw new Error("bad");
      setRequests(
        r.data?.requests || r.data?.data?.requests || r.data?.requests || []
      );
    } catch {
      toast.error("Failed to load requests");
    } finally {
      setReqLoading(false);
    }
  }

  async function loadChatMessages() {
    if (!chatUnlocked) return;

    setChatLoading(true);
    try {
      const r = await callJSON(
        `${API_BASE}/api/carry/listings/${listingId}/messages`,
        { headers: { ...authHeaders() } }
      );
      if (!r?.ok) throw new Error("bad");
      const msgs = r.data?.messages || r.data?.data?.messages || [];
      setMeta((m) => ({ ...m, messages: msgs }));
    } catch {
      toast.error("Failed to load chat");
    } finally {
      setChatLoading(false);
    }
  }

  useEffect(() => {
    if (!listingId) return;
    loadDetails();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [listingId]);

  useEffect(() => {
    if (isOwner) loadOwnerRequests();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isOwner, listingId]);

  useEffect(() => {
    if (!chatUnlocked) return;
    scrollChatToBottom("auto");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [chatUnlocked, meta.messages?.length]);

  // =========================
  // Actions
  // =========================
  async function onDelete() {
    if (!isOwner) return toast.error("Owner only.");
    const ok = await toastConfirm("Delete this listing?");
    if (!ok) return;

    try {
      const r = await callJSON(`${API_BASE}/api/carry/listings/${listingId}`, {
        method: "DELETE",
        headers: { ...authHeaders() },
      });
      if (r?.ok) {
        toast.success("Deleted ✅");
        nav("/carry");
      } else toast.error("Failed");
    } catch {
      toast.error("Failed");
    }
  }

  async function onRequestSubmit() {
    if (!authed) {
      toast.error("Please login first.");
      goLogin();
      return;
    }

    const noteOnly = noteDraft.trim();
    const note = buildOfferNote(noteOnly);

    try {
      const r = await callJSON(
        `${API_BASE}/api/carry/listings/${listingId}/request`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ note }),
        }
      );

      if (r?.ok) {
        const data = r.data || {};
        setMyReqRow(data.request || data.my_request || null);
        if (data.already) toast("You already requested ✅");
        else toast.success("Request sent ✅");

        setRequestModalOpen(false);
        resetOfferModal();

        await loadDetails();
        if (isOwner) await loadOwnerRequests();
      } else {
        toast.error(
          r?.data?.error || r?.data?.message || "Failed to send request"
        );
      }
    } catch {
      toast.error("Failed to send request");
    }
  }

  async function onAccept(requestId) {
    const r = await callFirstOk([
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/accept`,
        opts: { method: "POST", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/accept`,
        opts: { method: "PATCH", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/approve`,
        opts: { method: "POST", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/approve`,
        opts: { method: "PATCH", headers: { ...authHeaders() } },
      },
    ]);

    if (r?.ok) {
      toast.success("Accepted ✅");
      await loadOwnerRequests();
      await loadDetails();
      return;
    }

    if (r?.status === 404) {
      toast.error("Accept endpoint missing in backend (404).");
      return;
    }
    toast.error(r?.data?.error || r?.data?.message || "Failed");
  }

  async function onReject(requestId) {
    const r = await callFirstOk([
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/reject`,
        opts: { method: "POST", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/reject`,
        opts: { method: "PATCH", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/decline`,
        opts: { method: "POST", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${requestId}/decline`,
        opts: { method: "PATCH", headers: { ...authHeaders() } },
      },
    ]);

    if (r?.ok) {
      toast.success("Rejected ✅");
      await loadOwnerRequests();
      await loadDetails();
      return;
    }

    if (r?.status === 404) {
      toast.error("Reject endpoint missing in backend (404).");
      return;
    }
    toast.error(r?.data?.error || r?.data?.message || "Failed");
  }

  async function onCancelMyRequest() {
    if (!authed) {
      toast.error("Please login first.");
      goLogin();
      return;
    }

    const reqId =
      myReqRow?.id || meta?.my_request_id || meta?.my_request?.id || null;
    if (!reqId) return toast.error("No request to cancel.");

    const ok = await toastConfirm("Cancel your request?");
    if (!ok) return;

    const r = await callFirstOk([
      {
        url: `${API_BASE}/api/carry/requests/${reqId}`,
        opts: { method: "DELETE", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${reqId}/cancel`,
        opts: { method: "POST", headers: { ...authHeaders() } },
      },
      {
        url: `${API_BASE}/api/carry/requests/${reqId}/cancel`,
        opts: { method: "DELETE", headers: { ...authHeaders() } },
      },
    ]);

    if (r?.ok) {
      toast.success("Request canceled ✅");
      setMyReqRow(null);
      resetOfferModal();
      setRequestModalOpen(false);
      await loadDetails();
      return;
    }

    if (r?.status === 404) {
      toast.error("Cancel endpoint missing in backend (404).");
      return;
    }
    toast.error(r?.data?.error || r?.data?.message || "Cancel failed");
  }

  async function onUpdateMyNote() {
    if (!authed) {
      toast.error("Please login first.");
      goLogin();
      return;
    }

    const reqId =
      myReqRow?.id || meta?.my_request_id || meta?.my_request?.id || null;
    if (!reqId) return toast.error("Request not loaded");

    const noteOnly = noteDraft.trim();
    const note = buildOfferNote(noteOnly);

    if (!String(priceDraft || "").trim() && !noteOnly) {
      toast.error("Write your offer first");
      return;
    }

    const r = await callFirstOk([
      {
        url: `${API_BASE}/api/carry/requests/${reqId}/note`,
        opts: {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ note }),
        },
      },
      {
        url: `${API_BASE}/api/carry/requests/${reqId}/note`,
        opts: {
          method: "PUT",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ note }),
        },
      },
      {
        url: `${API_BASE}/api/carry/requests/${reqId}/offer`,
        opts: {
          method: "PATCH",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ note }),
        },
      },
    ]);

    if (r?.ok) {
      toast.success("Updated ✅");
      const data = r.data || {};
      if (data.request) setMyReqRow(data.request);
      setRequestModalOpen(false);
      resetOfferModal();
      await loadDetails();
      return;
    }

    if (r?.status === 404) {
      await onRequestSubmit();
      return;
    }

    toast.error(r?.data?.error || r?.data?.message || "Update failed");
  }

  async function onSendMessage() {
    if (!authed) {
      toast.error("Please login first.");
      goLogin();
      return;
    }
    if (!chatUnlocked) {
      toast.error("Chat is locked until request is accepted.");
      return;
    }

    const message = msgDraft.trim();
    if (!message) return;

    try {
      const r = await callJSON(
        `${API_BASE}/api/carry/listings/${listingId}/messages`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ message }),
        }
      );
      if (r?.ok) {
        setMsgDraft("");
        await loadChatMessages();
        scrollChatToBottom("smooth");
      } else {
        toast.error(r?.data?.error || r?.data?.message || "Failed");
      }
    } catch {
      toast.error("Failed");
    }
  }

  const routeLabel = useMemo(() => {
    const from = [item?.from_city, item?.from_country]
      .filter(Boolean)
      .join(", ");
    const to = [item?.to_city, item?.to_country].filter(Boolean).join(", ");
    return { from: from || "—", to: to || "—" };
  }, [item]);

  if (!listingId) {
    return (
      <div className="p-6">
        <div className="rounded-2xl border bg-white p-4">Bad listing id</div>
      </div>
    );
  }

  return (
    <div className="mx-auto w-full max-w-[1200px] px-4 py-6">
      {/* Top Bar */}
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <SecondaryButton onClick={() => nav(-1)}>
            <ArrowLeft className="h-4 w-4" />
            Back
          </SecondaryButton>

          {loading ? (
            <span className="text-sm opacity-60">Loading...</span>
          ) : (
            <span className="text-sm font-semibold">
              #{item?.id} <span className="opacity-60">•</span>{" "}
              <RolePill role={item?.role} />{" "}
              <span className="opacity-60">•</span>{" "}
              <StatusPill status={item?.status} />
            </span>
          )}
        </div>

        <div className="flex items-center gap-2">
          <SecondaryButton onClick={loadDetails}>
            <RefreshCw className="h-4 w-4" />
            Refresh
          </SecondaryButton>

          {isOwner ? (
            <>
              <SecondaryButton onClick={() => nav(`/carry/edit/${listingId}`)}>
                <Pencil className="h-4 w-4" />
                Edit
              </SecondaryButton>
              <DangerButton onClick={onDelete}>
                <Trash2 className="h-4 w-4" />
                Delete
              </DangerButton>
            </>
          ) : null}
        </div>
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-12">
        {/* Right column */}
        <div className="lg:col-span-7">
          <Card
            title="Route"
            icon={MapPin}
            right={
              <div className="text-xs opacity-70">
                Owner # {item?.user_id ?? "—"}
              </div>
            }
          >
            <div className="flex items-center justify-between gap-3 rounded-2xl border bg-slate-50 p-4">
              <div className="text-lg font-extrabold">{routeLabel.from}</div>
              <div className="text-sm font-semibold opacity-60">→</div>
              <div className="text-lg font-extrabold">{routeLabel.to}</div>
            </div>
          </Card>

          <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-3">
            <Card title="Date" icon={Calendar}>
              <div className="text-lg font-extrabold">
                {fmtDT(item?.travel_date)}
              </div>
              <div className="mt-1 text-xs opacity-70">Trip timing</div>
            </Card>

            <Card title="Weight" icon={Weight}>
              <div className="text-lg font-extrabold">
                {item?.available_weight != null
                  ? `kg ${item.available_weight}`
                  : "—"}
              </div>
              <div className="mt-1 text-xs opacity-70">Available capacity</div>
            </Card>

            <Card title="Reward" icon={DollarSign}>
              <div className="text-lg font-extrabold">
                {item?.reward_amount != null
                  ? `${item.currency || "USD"} ${item.reward_amount}`
                  : "—"}
              </div>
              <div className="mt-1 text-xs opacity-70">Negotiable</div>
            </Card>
          </div>

          <div className="mt-4 grid grid-cols-1 gap-4 md:grid-cols-2">
            <Card title="Item type" icon={User}>
              <div className="text-sm font-semibold">
                {item?.item_type || "—"}
              </div>
            </Card>

            <Card
              title="Rating"
              icon={User}
              right={
                <span className="text-xs opacity-70">
                  ({meta.reviews_count || 0})
                </span>
              }
            >
              <div className="text-sm font-semibold">
                {Number(meta.avg_rating || 0).toFixed(1)}
              </div>
            </Card>
          </div>

          {/* summary card */}
          <div className="mt-4">
            <div className="rounded-2xl border border-slate-200 bg-white shadow-sm overflow-hidden">
              <div className="p-5 border-b border-slate-100 bg-gradient-to-b from-white to-slate-50">
                <div className="flex items-start justify-between gap-4">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="text-xs font-extrabold text-slate-500 uppercase tracking-wide">
                        Route
                      </span>

                      {item?.role ? (
                        <span
                          className={cn(
                            "px-3 py-1 rounded-full text-xs font-extrabold border",
                            String(item.role).toLowerCase() === "traveler"
                              ? "bg-violet-50 text-violet-700 border-violet-100"
                              : "bg-amber-50 text-amber-800 border-amber-100"
                          )}
                        >
                          {String(item.role).toLowerCase() === "traveler"
                            ? "Traveler"
                            : "Sender"}
                        </span>
                      ) : null}

                      {item?.updated_at ? (
                        <span className="text-xs text-slate-500">
                          • Updated {fmtDT(item.updated_at)}
                        </span>
                      ) : null}
                    </div>

                    <div className="mt-2 text-2xl font-black text-slate-900">
                      {[item?.from_city, item?.from_country]
                        .filter(Boolean)
                        .join(", ") || "—"}
                      <span className="text-slate-300 mx-2">→</span>
                      {[item?.to_city, item?.to_country]
                        .filter(Boolean)
                        .join(", ") || "—"}
                    </div>
                  </div>

                  <div className="shrink-0 text-right">
                    <div className="text-xs font-extrabold text-slate-500 uppercase tracking-wide">
                      Reward
                    </div>
                    <div className="mt-1 text-3xl font-black text-slate-900">
                      {item?.reward_amount != null
                        ? `${item.currency || "USD"} ${item.reward_amount}`
                        : "—"}
                    </div>

                    <div className="mt-3 text-xs font-extrabold text-slate-500 uppercase tracking-wide">
                      Capacity
                    </div>
                    <div className="mt-1 text-sm font-extrabold text-slate-900">
                      {item?.available_weight != null
                        ? `${item.available_weight} kg`
                        : "—"}
                    </div>
                  </div>
                </div>
              </div>

              <div className="px-5 pb-5 pt-5">
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="text-xs font-extrabold text-slate-500 uppercase tracking-wide">
                    Description
                  </div>
                  <div className="mt-2 whitespace-pre-wrap text-sm leading-7 text-slate-800">
                    {item?.description || "No description provided."}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Left column */}
        <div className="lg:col-span-5">
          {/* ✅ Request/Status panel (NO CHAT HERE) */}
          <Card
            title="Request status"
            icon={SendHorizontal}
            right={
              !authed ? (
                <span className={pillBase("bg-slate-50 text-slate-700 border")}>
                  <Lock className="h-3.5 w-3.5" /> Login required
                </span>
              ) : isOwner ? (
                <span
                  className={pillBase(
                    "bg-emerald-50 text-emerald-700 border border-emerald-100"
                  )}
                >
                  Owner
                </span>
              ) : myStatus ? (
                <span
                  className={pillBase(
                    myStatus === "pending"
                      ? "bg-amber-50 text-amber-800 border border-amber-100"
                      : myStatus === "accepted"
                      ? "bg-emerald-50 text-emerald-700 border border-emerald-100"
                      : "bg-slate-50 text-slate-700 border border-slate-100"
                  )}
                >
                  {myStatus}
                </span>
              ) : (
                <span className={pillBase("bg-slate-50 text-slate-700 border")}>
                  none
                </span>
              )
            }
          >
            <div className="text-sm opacity-80">
              {!authed ? (
                <div className="flex items-center justify-between gap-3">
                  <span>You must login to send a request.</span>
                  <LoginLinkButton onClick={goLogin} />
                </div>
              ) : isOwner ? (
                "You are the owner. You can accept/reject incoming requests below."
              ) : myStatus === "accepted" ? (
                "Accepted ✅ You can now chat in the Messages section."
              ) : myStatus === "pending" ? (
                "Pending… Waiting for owner approval. Chat is locked until accepted."
              ) : (
                "Send a request first. Chat will unlock only after acceptance."
              )}
            </div>

            <div className="mt-3 flex items-center justify-between">
              <div className="text-sm">
                <span className="font-semibold">Requests:</span>{" "}
                <span className="opacity-80">{meta.requests_count}</span>
              </div>

              {!loading && !!item && !isOwner ? (
                <div className="flex items-center gap-2">
                  {!authed ? (
                    <LoginLinkButton onClick={goLogin} />
                  ) : (
                    <>
                      {myStatus === "pending" ? (
                        <>
                          <SecondaryButton
                            onClick={() => {
                              setOfferStep(1);
                              setPriceDraft("");
                              setCurrencyDraft("USD");
                              setNoteDraft(
                                String(
                                  myReqRow?.note || meta?.my_request?.note || ""
                                )
                              );
                              setRequestModalOpen(true);
                            }}
                          >
                            <SendHorizontal className="h-4 w-4" />
                            Edit offer
                          </SecondaryButton>

                          <DangerButton onClick={onCancelMyRequest}>
                            <XCircle className="h-4 w-4" />
                            Cancel
                          </DangerButton>
                        </>
                      ) : myStatus ? null : (
                        <PrimaryButton
                          onClick={() => {
                            resetOfferModal();
                            setRequestModalOpen(true);
                          }}
                        >
                          <SendHorizontal className="h-4 w-4" />
                          Request
                        </PrimaryButton>
                      )}
                    </>
                  )}
                </div>
              ) : null}
            </div>
          </Card>

          {/* Owner Requests Panel */}
          {isOwner ? (
            <Card
              title="Requests received"
              icon={User}
              right={
                reqLoading ? (
                  <span className="text-sm opacity-60">Loading...</span>
                ) : (
                  <SecondaryButton onClick={loadOwnerRequests}>
                    <RefreshCw className="h-4 w-4" />
                    Refresh
                  </SecondaryButton>
                )
              }
            >
              {!requests?.length ? (
                <div className="text-sm opacity-70">No requests yet.</div>
              ) : (
                <div className="space-y-3">
                  {requests.map((r) => {
                    const st =
                      normReqStatus(r.status || "pending") || "pending";
                    const stCls =
                      st === "accepted"
                        ? "bg-emerald-50 text-emerald-700 border border-emerald-100"
                        : st === "rejected"
                        ? "bg-rose-50 text-rose-700 border border-rose-100"
                        : "bg-slate-50 text-slate-700 border border-slate-100";

                    const requesterLabel =
                      r.requester_name ||
                      r.requester_username ||
                      r.requester_email ||
                      `Requester #${r.requester_id}`;

                    return (
                      <div key={r.id} className="rounded-xl border p-3">
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="flex items-center gap-2">
                              <span className="text-sm font-semibold">
                                {requesterLabel}
                              </span>
                              <span className={pillBase(stCls)}>{st}</span>
                            </div>
                            <div className="mt-1 text-xs opacity-70">
                              Created: {fmtDT(r.created_at)}{" "}
                              {r.updated_at ? (
                                <>• Updated: {fmtDT(r.updated_at)}</>
                              ) : null}
                            </div>

                            {r.note ? (
                              <div className="mt-2 rounded-lg bg-slate-50 p-2 text-sm">
                                <div className="text-xs font-semibold opacity-70">
                                  Offer / Note
                                </div>
                                <div className="mt-1 whitespace-pre-wrap">
                                  {r.note}
                                </div>
                              </div>
                            ) : (
                              <div className="mt-2 text-sm opacity-70">
                                No note.
                              </div>
                            )}
                          </div>

                          <div className="flex flex-col gap-2">
                            <PrimaryButton
                              disabled={st !== "pending"}
                              onClick={() => onAccept(r.id)}
                              className="bg-emerald-600 hover:bg-emerald-700 active:bg-emerald-800"
                            >
                              <CheckCircle2 className="h-4 w-4" />
                              Accept
                            </PrimaryButton>
                            <DangerButton
                              disabled={st !== "pending"}
                              onClick={() => onReject(r.id)}
                            >
                              <XCircle className="h-4 w-4" />
                              Reject
                            </DangerButton>
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </Card>
          ) : null}

          {/* ✅ Messages panel (ONLY AFTER ACCEPTED) */}
          <Card
            title="Messages"
            icon={MessageCircle}
            right={
              chatUnlocked ? (
                <SecondaryButton
                  onClick={loadChatMessages}
                  disabled={chatLoading}
                >
                  <RefreshCw className="h-4 w-4" />
                  Refresh
                </SecondaryButton>
              ) : (
                <span
                  className={pillBase(
                    "bg-slate-50 text-slate-700 border border-slate-100"
                  )}
                >
                  <Lock className="h-3.5 w-3.5" /> Locked
                </span>
              )
            }
          >
            {!chatUnlocked ? (
              <div className="text-sm opacity-70">
                {!authed
                  ? "Login first. Then send a request. Chat opens only after acceptance."
                  : isOwner
                  ? "Accept a request to unlock chat for that requester."
                  : myStatus === "pending"
                  ? "Waiting for owner approval…"
                  : "Send a request first."}
              </div>
            ) : (
              <>
                <div className="max-h-[260px] space-y-2 overflow-auto rounded-xl border bg-slate-50 p-3">
                  {chatLoading ? (
                    <div className="text-sm opacity-60">Loading...</div>
                  ) : meta.messages?.length ? (
                    meta.messages.slice().map((m) => {
                      const mine =
                        myUserId != null &&
                        Number(m.sender_id) === Number(myUserId);

                      const senderName =
                        m.sender_name ||
                        m.sender_username ||
                        m.sender_email ||
                        `User #${m.sender_id}`;

                      return (
                        <div
                          key={m.id}
                          className={cn(
                            "flex",
                            mine ? "justify-end" : "justify-start"
                          )}
                        >
                          <div
                            className={cn(
                              "max-w-[78%] rounded-2xl px-3 py-2 text-sm shadow-sm",
                              mine
                                ? "bg-slate-900 text-white"
                                : "bg-white text-slate-900 border border-slate-200"
                            )}
                          >
                            <div
                              className={cn(
                                "mb-1 flex items-center gap-2 text-[11px]",
                                mine ? "text-white/70" : "text-slate-500"
                              )}
                            >
                              <span className="font-semibold truncate max-w-[180px]">
                                {mine ? "You" : senderName}
                              </span>
                              <span className="opacity-50">•</span>
                              <span className="whitespace-nowrap">
                                {fmtDT(m.created_at)}
                              </span>
                            </div>

                            <div className="whitespace-pre-wrap leading-6">
                              {m.message}
                            </div>
                          </div>
                        </div>
                      );
                    })
                  ) : (
                    <div className="text-sm opacity-60">No messages yet.</div>
                  )}

                  <div ref={chatEndRef} />
                </div>

                <div className="mt-3 flex gap-2">
                  <input
                    className="h-11 flex-1 rounded-xl border px-3 text-sm outline-none focus:ring-2 focus:ring-slate-200"
                    placeholder={
                      !authed ? "Login to chat..." : "Write a message..."
                    }
                    value={msgDraft}
                    disabled={!authed || !chatUnlocked}
                    onChange={(e) => setMsgDraft(e.target.value)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter" && !e.shiftKey) onSendMessage();
                    }}
                  />

                  {!authed ? (
                    <LoginLinkButton onClick={goLogin} className="h-11" />
                  ) : (
                    <PrimaryButton
                      onClick={onSendMessage}
                      disabled={!chatUnlocked}
                    >
                      <SendHorizontal className="h-4 w-4" />
                      Send
                    </PrimaryButton>
                  )}
                </div>
              </>
            )}
          </Card>
        </div>
      </div>

      {/* Request / Offer modal */}
      <Modal
        open={requestModalOpen}
        title={myStatus === "pending" ? "Update your offer" : "Send request"}
        onClose={() => {
          setRequestModalOpen(false);
          resetOfferModal();
        }}
      >
        {!authed ? (
          <div className="space-y-4">
            <div className="rounded-xl border bg-slate-50 p-3 text-sm">
              لازم تعمل Login الأول علشان تبعت Request.
            </div>
            <div className="flex justify-end">
              <LoginLinkButton onClick={goLogin} />
            </div>
          </div>
        ) : (
          <div className="space-y-4">
            <div className="flex items-center gap-2 text-xs font-extrabold">
              <div
                className={cn(
                  "flex items-center gap-2 rounded-full px-3 py-1 border",
                  offerStep === 1
                    ? "bg-slate-900 text-white border-slate-900"
                    : "bg-white text-slate-700 border-slate-200"
                )}
              >
                1) Offer
              </div>
              <div className="opacity-40">→</div>
              <div
                className={cn(
                  "flex items-center gap-2 rounded-full px-3 py-1 border",
                  offerStep === 2
                    ? "bg-slate-900 text-white border-slate-900"
                    : "bg-white text-slate-700 border-slate-200"
                )}
              >
                2) Details
              </div>
              <div className="ml-auto text-[11px] opacity-70">
                Chat opens after accept ✅
              </div>
            </div>

            {offerStep === 1 ? (
              <>
                <div className="text-sm opacity-80">
                  اكتب السعر الأول (ده أساس الدفع بعد القبول).
                </div>

                <div className="grid grid-cols-3 gap-2">
                  <select
                    className="h-11 rounded-xl border px-3 text-sm outline-none focus:ring-2 focus:ring-slate-200"
                    value={currencyDraft}
                    onChange={(e) => setCurrencyDraft(e.target.value)}
                  >
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                    <option value="EGP">EGP</option>
                    <option value="GBP">GBP</option>
                  </select>

                  <input
                    className="h-11 col-span-2 rounded-xl border px-3 text-sm outline-none focus:ring-2 focus:ring-slate-200"
                    placeholder="Offer price (e.g. 50)"
                    value={priceDraft}
                    onChange={(e) => setPriceDraft(e.target.value)}
                    inputMode="decimal"
                  />
                </div>

                <div className="rounded-xl border bg-slate-50 p-3 text-sm">
                  بعد ما تبعت Offer، الشات هيفضل مقفول لحد ما الطرف التاني يقبل
                  ✅
                </div>

                <div className="flex justify-end gap-2">
                  <SecondaryButton
                    onClick={() => {
                      setRequestModalOpen(false);
                      resetOfferModal();
                    }}
                  >
                    Cancel
                  </SecondaryButton>

                  <PrimaryButton
                    onClick={() => setOfferStep(2)}
                    disabled={!String(priceDraft || "").trim()}
                  >
                    Next
                  </PrimaryButton>
                </div>
              </>
            ) : (
              <>
                <div className="text-sm opacity-80">
                  اكتب تفاصيل الاتفاق (مكان التسليم، الوقت، ملاحظات…)
                </div>

                <textarea
                  className="mt-2 h-32 w-full resize-none rounded-xl border px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-slate-200"
                  placeholder="Details..."
                  value={noteDraft}
                  onChange={(e) => setNoteDraft(e.target.value)}
                />

                <div className="flex justify-between gap-2">
                  <SecondaryButton onClick={() => setOfferStep(1)}>
                    Back
                  </SecondaryButton>

                  <div className="flex gap-2">
                    <SecondaryButton
                      onClick={() => {
                        setRequestModalOpen(false);
                        resetOfferModal();
                      }}
                    >
                      Cancel
                    </SecondaryButton>

                    {myStatus === "pending" ? (
                      <PrimaryButton
                        onClick={onUpdateMyNote}
                        disabled={!String(priceDraft || "").trim()}
                      >
                        Update Offer
                      </PrimaryButton>
                    ) : (
                      <PrimaryButton
                        onClick={onRequestSubmit}
                        disabled={!String(priceDraft || "").trim()}
                      >
                        Send Request
                      </PrimaryButton>
                    )}
                  </div>
                </div>
              </>
            )}
          </div>
        )}
      </Modal>
    </div>
  );
}
