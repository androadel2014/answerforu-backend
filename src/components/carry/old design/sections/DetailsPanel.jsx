// src/components/carry/sections/DetailsPanel.jsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import toast from "react-hot-toast";
import {
  Plane,
  SendHorizontal,
  Star,
  Calendar,
  MapPin,
  Weight,
  DollarSign,
  MessageCircle,
  Lock,
  Clock3,
  CheckCircle2,
  XCircle,
  RefreshCw,
} from "lucide-react";

import { cn, getCurrentUserId } from "../shared/carryUtils";
import {
  fetchCarryRequests,
  acceptCarryRequest,
  rejectCarryRequest,
  fetchCarryDetails,
} from "../shared/carryApi";

function normReqStatus(v) {
  const s = String(v || "")
    .toLowerCase()
    .trim();
  if (!s) return null;
  if (s === "sent") return "pending";
  if (s === "approved" || s === "matched") return "accepted";
  if (s === "cancelled") return "canceled";
  return s;
}

export default function DetailsPanel({
  loading,
  details,
  messages = [],
  msg,
  setMsg,
  onSend,
  onRequest,
  onEdit,
  onDelete,
  currentUserId: currentUserIdProp,
}) {
  const it = details?.item;
  if (!it) return null;

  const currentUserId = currentUserIdProp ?? getCurrentUserId();

  const ownerId =
    it?.owner_id ?? it?.user_id ?? it?.created_by ?? it?.createdBy ?? null;

  const isOwnerLocal =
    currentUserId && String(ownerId) === String(currentUserId);

  // ✅ server flags (from backend /api/carry/listings/:id)
  const canChatServer = !!details?.can_chat; // backend may set true on match
  const isOwnerServer = details?.is_owner === true;

  // ✅ normalize my status
  const myStatus = normReqStatus(details?.my_request_status || null); // pending|accepted|rejected|canceled|null

  const isOwnerView = isOwnerServer || isOwnerLocal;

  // ✅ HARD RULE: chat only when accepted (or owner view with an accepted match)
  const canChat = canChatServer && (isOwnerView || myStatus === "accepted");

  const isTraveler = String(it?.role || "").toLowerCase() === "traveler";

  const currency = it?.currency || "USD";
  const travelText = it?.travel_date || it?.arrival_date || "—";

  const w = it?.available_weight ?? it?.weight ?? it?.max_weight ?? null;
  const weightText =
    w === null || w === undefined || w === "" ? "—" : `${w} kg`;

  const rRaw =
    it?.reward_amount ??
    it?.reward ??
    it?.reward_value ??
    it?.price_value ??
    it?.price ??
    null;

  const rNum = Number(rRaw || 0) || 0;
  const rewardText =
    rRaw === null || rRaw === undefined || String(rRaw).trim() === ""
      ? "—"
      : `${currency} ${rRaw}`;

  const pct = Number(it?.negotiation_pct ?? 0.15);
  const hasReward = rNum > 0;
  const minOffer = hasReward ? Math.max(0, Math.floor(rNum * (1 - pct))) : null;
  const maxOffer = hasReward ? Math.ceil(rNum * (1 + pct)) : null;

  const fromTxt =
    [it?.from_city, it?.from_country].filter(Boolean).join(", ") || "—";
  const toTxt = [it?.to_city, it?.to_country].filter(Boolean).join(", ") || "—";

  const statusIcon =
    myStatus === "pending" ? (
      <Clock3 size={16} />
    ) : myStatus === "accepted" ? (
      <CheckCircle2 size={16} />
    ) : myStatus === "rejected" ? (
      <XCircle size={16} />
    ) : null;

  const statusText =
    myStatus === "pending"
      ? "Request pending"
      : myStatus === "accepted"
      ? "Request accepted"
      : myStatus === "rejected"
      ? "Request rejected"
      : myStatus === "canceled"
      ? "Request canceled"
      : "No request yet";

  // =====================
  // OWNER: Requests list
  // =====================
  const [reqLoading, setReqLoading] = useState(false);
  const [requests, setRequests] = useState([]);

  async function loadRequests() {
    if (!isOwnerView || !it?.id) return;
    setReqLoading(true);
    try {
      const r = await fetchCarryRequests(it.id);
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const root = r?.data ?? r;
      const rows = Array.isArray(root?.requests) ? root.requests : [];
      setRequests(rows);
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setReqLoading(false);
    }
  }

  async function refreshDetailsNow() {
    if (!it?.id) return;
    try {
      const fresh = await fetchCarryDetails(it.id);
      if (!fresh?.ok) return;
      const root = fresh?.data ?? fresh;

      window.dispatchEvent(
        new CustomEvent("carry:detailsUpdated", { detail: { details: root } })
      );
    } catch {
      // silent
    }
  }

  useEffect(() => {
    if (isOwnerView && !canChat) loadRequests();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [it?.id, isOwnerView, canChat]);

  async function onAccept(reqId) {
    try {
      setReqLoading(true);
      const r = await acceptCarryRequest(reqId);
      if (!r?.ok) throw new Error(r?.error || "Failed");

      toast.success("Accepted ✅");
      await loadRequests();
      await refreshDetailsNow(); // ✅ unlock chat when details says so
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setReqLoading(false);
    }
  }

  async function onReject(reqId) {
    try {
      setReqLoading(true);
      const r = await rejectCarryRequest(reqId);
      if (!r?.ok) throw new Error(r?.error || "Failed");

      toast.success("Rejected ✅");
      await loadRequests();
      await refreshDetailsNow();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setReqLoading(false);
    }
  }

  const ratingText = useMemo(() => {
    const avg = Number(details?.avg_rating || 0);
    const c = Number(details?.reviews_count || 0);
    return `${avg.toFixed(1)} (${c})`;
  }, [details?.avg_rating, details?.reviews_count]);

  // =====================
  // CHAT: Facebook-like
  // =====================
  const chatEndRef = useRef(null);
  const chatListRef = useRef(null);

  function scrollChatToBottom(behavior = "auto") {
    try {
      chatEndRef.current?.scrollIntoView({ behavior, block: "end" });
    } catch {}
  }

  useEffect(() => {
    if (!canChat) return;
    scrollChatToBottom("auto");
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [canChat, messages?.length]);

  function senderLabel(m, isMe) {
    if (isMe) return "You";
    return (
      m?.sender_name ||
      m?.sender_username ||
      m?.sender_email ||
      (m?.sender_id != null ? `User #${m.sender_id}` : "User")
    );
  }

  function timeLabel(m) {
    return m?.created_at || m?.createdAt || "";
  }

  // ACTION BUTTON LABEL (hide after pending)
  const requestBtnLabel =
    myStatus === "pending"
      ? "Request Pending"
      : myStatus === "rejected" || myStatus === "canceled"
      ? "Request Again"
      : myStatus === "accepted"
      ? "Accepted"
      : "Request Match";

  const requestBtnDisabled =
    loading || myStatus === "pending" || myStatus === "accepted";

  return (
    <div className="rounded-3xl border border-slate-200 bg-white overflow-hidden shadow-sm">
      {/* HEADER */}
      <div className="p-5 border-b border-slate-100">
        <div className="flex items-start justify-between gap-3">
          <div className="flex items-center gap-2">
            <div
              className={cn(
                "w-11 h-11 rounded-2xl flex items-center justify-center",
                isTraveler
                  ? "bg-indigo-50 text-indigo-700"
                  : "bg-emerald-50 text-emerald-700"
              )}
            >
              {isTraveler ? <Plane size={18} /> : <SendHorizontal size={18} />}
            </div>

            <div>
              <div className="text-base font-black text-slate-900">
                {isTraveler ? "Traveler" : "Sender"} • #{it.id}
              </div>
              <div className="text-xs text-slate-500 mt-0.5">
                Owner: #{ownerId ?? "—"} • Status: {it.status || "open"}
              </div>
            </div>
          </div>

          <div className="text-xs font-extrabold px-2.5 py-1 rounded-full bg-slate-100 text-slate-700">
            {ratingText} <Star size={14} className="inline -mt-0.5" />
          </div>
        </div>

        <div className="mt-4 flex items-start gap-2 text-sm text-slate-800">
          <MapPin size={16} className="mt-0.5 text-slate-400" />
          <div className="font-semibold">
            {fromTxt} <span className="text-slate-400">→</span> {toTxt}
          </div>
        </div>

        <div className="mt-4 grid grid-cols-3 gap-2 text-xs">
          <div className="rounded-2xl border border-slate-200 bg-slate-50 px-2.5 py-2">
            <div className="flex items-center gap-1 text-slate-500 font-bold">
              <Calendar size={14} /> Travel
            </div>
            <div className="mt-1 font-extrabold text-slate-900">
              {travelText}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-slate-50 px-2.5 py-2">
            <div className="flex items-center gap-1 text-slate-500 font-bold">
              <Weight size={14} /> Weight
            </div>
            <div className="mt-1 font-extrabold text-slate-900">
              {weightText}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-slate-50 px-2.5 py-2">
            <div className="flex items-center gap-1 text-slate-500 font-bold">
              <DollarSign size={14} /> Reward
            </div>
            <div className="mt-1 font-extrabold text-slate-900">
              {rewardText}
              {hasReward && minOffer != null && maxOffer != null ? (
                <div className="mt-1 text-[11px] text-slate-500 font-semibold">
                  Negotiable: {currency} {minOffer}–{maxOffer}
                </div>
              ) : null}
            </div>
          </div>
        </div>

        <div className="mt-4 text-sm text-slate-700">
          <div className="font-extrabold text-slate-900">
            {it.item_type || "Item"}
          </div>
          <div className="mt-1">{it.description || "—"}</div>
        </div>

        {/* ACTIONS */}
        <div className="mt-5 flex gap-2">
          {!isOwnerLocal ? (
            myStatus === "accepted" ? (
              <button
                disabled
                className="flex-1 px-4 py-3 rounded-2xl bg-slate-200 text-slate-700 font-extrabold"
                title="Already accepted"
              >
                Accepted ✅
              </button>
            ) : (
              <button
                disabled={requestBtnDisabled}
                onClick={onRequest}
                className={cn(
                  "flex-1 px-4 py-3 rounded-2xl text-white font-extrabold disabled:opacity-60",
                  myStatus === "pending"
                    ? "bg-slate-400"
                    : "bg-indigo-600 hover:bg-indigo-700"
                )}
              >
                {requestBtnLabel}
              </button>
            )
          ) : (
            <button
              disabled={loading}
              onClick={onEdit}
              className="flex-1 px-4 py-3 rounded-2xl border border-slate-200 font-extrabold hover:bg-slate-50 disabled:opacity-60"
            >
              Edit
            </button>
          )}

          {isOwnerLocal && typeof onDelete === "function" ? (
            <button
              disabled={loading}
              onClick={onDelete}
              className="px-4 py-3 rounded-2xl border border-red-200 text-red-700 font-extrabold hover:bg-red-50 disabled:opacity-60"
              title="Owner only"
            >
              Delete
            </button>
          ) : null}
        </div>

        <div className="mt-3 text-xs text-slate-500 font-bold">
          Requests: {details?.requests_count || 0}
        </div>
      </div>

      {/* CHAT */}
      <div className="p-5">
        <div className="flex items-center justify-between">
          <div className="text-sm font-black text-slate-900 flex items-center gap-2">
            <MessageCircle size={16} />
            Chat
          </div>
          <div className="text-xs text-slate-500 font-bold">
            {canChat ? `Messages: ${messages.length}` : "Locked"}
          </div>
        </div>

        {/* LOCKED */}
        {!canChat ? (
          <div className="mt-3 rounded-2xl border border-dashed border-slate-200 bg-slate-50 p-4">
            <div className="flex items-start gap-3">
              <div className="w-10 h-10 rounded-2xl bg-white border border-slate-200 flex items-center justify-center text-slate-700">
                <Lock size={18} />
              </div>

              <div className="flex-1">
                <div className="text-sm font-black text-slate-900">
                  Chat is private
                </div>

                {isOwnerView ? (
                  <>
                    <div className="mt-1 text-sm text-slate-500">
                      Chat opens only after you{" "}
                      <span className="font-extrabold text-slate-900">
                        accept
                      </span>{" "}
                      a request.
                    </div>

                    <div className="mt-3 flex items-center justify-between">
                      <div className="text-xs text-slate-500 font-bold">
                        Requests list
                      </div>
                      <button
                        disabled={reqLoading}
                        onClick={loadRequests}
                        className="px-3 py-2 rounded-xl border border-slate-200 bg-white hover:bg-slate-50 text-xs font-extrabold inline-flex items-center gap-2 disabled:opacity-60"
                      >
                        <RefreshCw size={14} />
                        Refresh
                      </button>
                    </div>

                    <div className="mt-2 rounded-2xl border border-slate-200 bg-white p-3 space-y-2">
                      {reqLoading ? (
                        <div className="text-sm text-slate-500">Loading…</div>
                      ) : requests?.length ? (
                        requests.map((rr) => (
                          <div
                            key={rr.id}
                            className="rounded-2xl border border-slate-200 bg-slate-50 p-3"
                          >
                            <div className="flex items-center justify-between gap-2">
                              <div className="text-sm font-extrabold text-slate-900">
                                {rr.requester_name ||
                                  rr.requester_username ||
                                  rr.requester_email ||
                                  `User #${rr.requester_id}`}
                              </div>
                              <div className="text-xs font-bold text-slate-600">
                                {normReqStatus(rr.status) || rr.status}
                              </div>
                            </div>

                            {rr.note ? (
                              <div className="mt-2 text-xs text-slate-600 whitespace-pre-wrap">
                                {rr.note}
                              </div>
                            ) : null}

                            <div className="mt-2 flex gap-2">
                              <button
                                disabled={
                                  reqLoading ||
                                  normReqStatus(rr.status) !== "pending"
                                }
                                onClick={() => onAccept(rr.id)}
                                className="flex-1 px-3 py-2 rounded-xl bg-emerald-600 text-white font-extrabold text-sm hover:bg-emerald-700 disabled:opacity-60"
                              >
                                Accept
                              </button>
                              <button
                                disabled={
                                  reqLoading ||
                                  normReqStatus(rr.status) !== "pending"
                                }
                                onClick={() => onReject(rr.id)}
                                className="flex-1 px-3 py-2 rounded-xl border border-red-200 bg-white text-red-700 font-extrabold text-sm hover:bg-red-50 disabled:opacity-60"
                              >
                                Reject
                              </button>
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-slate-500">
                          No requests yet.
                        </div>
                      )}
                    </div>
                  </>
                ) : (
                  <>
                    <div className="mt-1 text-sm text-slate-500 flex items-center gap-2">
                      {statusIcon}
                      <span>{statusText}</span>
                    </div>

                    <button
                      disabled={requestBtnDisabled}
                      onClick={onRequest}
                      className={cn(
                        "mt-3 w-full px-4 py-3 rounded-2xl text-white font-extrabold disabled:opacity-60",
                        myStatus === "pending"
                          ? "bg-slate-400"
                          : "bg-indigo-600 hover:bg-indigo-700"
                      )}
                    >
                      {requestBtnLabel}
                    </button>
                  </>
                )}
              </div>
            </div>
          </div>
        ) : (
          <>
            {/* ✅ BUBBLES (newest at bottom) + autoscroll */}
            <div
              ref={chatListRef}
              className="mt-3 max-h-64 overflow-auto rounded-2xl border border-slate-200 bg-slate-50 p-3 space-y-2"
            >
              {messages.length ? (
                messages.filter(Boolean).map((m, idx) => {
                  const isMe =
                    String(m?.sender_id ?? "") === String(currentUserId);

                  const name = senderLabel(m, isMe);
                  const time = timeLabel(m);

                  return (
                    <div
                      key={m?.id ?? `${idx}`}
                      className={cn(
                        "flex",
                        isMe ? "justify-end" : "justify-start"
                      )}
                    >
                      <div
                        className={cn(
                          "max-w-[78%] rounded-2xl px-4 py-2 text-sm shadow-sm",
                          isMe
                            ? "bg-slate-900 text-white rounded-br-sm"
                            : "bg-white border border-slate-200 text-slate-900 rounded-bl-sm"
                        )}
                      >
                        <div
                          className={cn(
                            "mb-1 flex items-center gap-2 text-[11px]",
                            isMe ? "text-white/70" : "text-slate-500"
                          )}
                        >
                          <span className="font-semibold truncate max-w-[180px]">
                            {name}
                          </span>
                          <span className="opacity-50">•</span>
                          <span className="whitespace-nowrap">{time}</span>
                        </div>

                        <div className="whitespace-pre-wrap break-words leading-6">
                          {m?.message ?? ""}
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="text-sm text-slate-500">No messages yet.</div>
              )}

              <div ref={chatEndRef} />
            </div>

            {/* ✅ INPUT */}
            <div className="mt-3 flex gap-2">
              <input
                value={msg}
                onChange={(e) => setMsg(e.target.value)}
                placeholder="Write message…"
                className="flex-1 px-3 py-2.5 rounded-2xl border border-slate-200 bg-white outline-none focus:ring-2 focus:ring-slate-200"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    onSend?.();
                    setTimeout(() => scrollChatToBottom("smooth"), 80);
                  }
                }}
              />
              <button
                disabled={!msg.trim()}
                onClick={() => {
                  onSend?.();
                  setTimeout(() => scrollChatToBottom("smooth"), 80);
                }}
                className="px-4 py-2.5 rounded-2xl bg-slate-900 text-white font-extrabold hover:bg-slate-800 disabled:opacity-50"
              >
                Send
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
