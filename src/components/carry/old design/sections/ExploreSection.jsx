// src/components/carry/sections/ExploreSection.jsx
import React, { useMemo } from "react";
import {
  Package,
  Search,
  Filter,
  RefreshCw,
  Plane,
  SendHorizontal,
  MapPin,
  Calendar,
  Weight,
  DollarSign,
  ArrowRight,
  Sparkles,
  Send,
  LogIn,
  Hourglass,
  CheckCircle2,
  MessageCircle,
  XCircle,
} from "lucide-react";

import { cn } from "../shared/carryUtils";

export default function ExploreSection({
  loading,
  items,
  selectedId,
  q,
  setQ,
  onOpenFilters,
  onSearch,
  onOpenDetails,
  currentUserId,
  onEditFromCard,
  onDeleteFromCard,
  onRequestFromCard, // ✅ now opens modal (choose shipment + offer)
  onGoLogin,
  DetailsSlot,
}) {
  const empty = !items?.length;
  const list = useMemo(() => items || [], [items]);

  function getRequestState(it) {
    const raw =
      it.my_request_status ||
      it.myRequestStatus ||
      it.request_status ||
      it.requestStatus ||
      it.request_state ||
      it.requestState ||
      it.request?.status ||
      it.request?.state ||
      null;

    const s = String(raw || "").toLowerCase();
    if (s === "pending" || s === "sent") return "pending";
    if (s === "accepted" || s === "approved" || s === "matched")
      return "accepted";
    if (
      s === "rejected" ||
      s === "declined" ||
      s === "canceled" ||
      s === "cancelled"
    )
      return "rejected";
    return "none";
  }

  return (
    <div className="p-4 sm:p-6">
      <div className="flex flex-col lg:flex-row gap-3 lg:items-center lg:justify-between">
        <div className="flex flex-col sm:flex-row gap-2 flex-1">
          <div className="flex-1 relative">
            <Search
              size={18}
              className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400"
            />
            <input
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Search trips (city/country/type)…"
              className="w-full pl-10 pr-3 py-2.5 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
            />
          </div>

          <button
            onClick={onOpenFilters}
            className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold flex items-center gap-2"
          >
            <Filter size={16} />
            Filters
          </button>

          <button
            disabled={loading}
            onClick={onSearch}
            className="px-5 py-2.5 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold disabled:opacity-60"
          >
            Search
          </button>
        </div>

        <div className="hidden lg:flex items-center gap-2">
          <button
            disabled={loading}
            onClick={onSearch}
            className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold flex items-center gap-2 disabled:opacity-60"
          >
            <RefreshCw size={16} />
            Refresh
          </button>
        </div>
      </div>

      <div className="mt-5 grid lg:grid-cols-3 gap-4">
        <div className="lg:col-span-2 space-y-3">
          {empty ? (
            <div className="rounded-3xl border border-slate-200 bg-slate-50 p-8 text-center">
              <div className="mx-auto w-14 h-14 rounded-2xl bg-white border border-slate-200 flex items-center justify-center">
                <Package size={22} className="text-slate-500" />
              </div>
              <div className="mt-3 text-lg font-black text-slate-900">
                No trips yet
              </div>
              <div className="mt-1 text-sm text-slate-500">
                Be the first to post a trip.
              </div>
            </div>
          ) : (
            <div className="grid sm:grid-cols-2 gap-3">
              {list.map((it) => (
                <ListingCard
                  key={it.id}
                  it={it}
                  selected={Number(selectedId) === Number(it.id)}
                  currentUserId={currentUserId}
                  loading={loading}
                  requestState={getRequestState(it)}
                  onOpen={() => onOpenDetails(it.id)}
                  onEdit={() => onEditFromCard(it)}
                  onDelete={() => onDeleteFromCard?.(it)}
                  onRequest={() => onRequestFromCard?.(it.id)}
                  onLogin={() => onGoLogin?.()}
                  onNext={() => onOpenDetails?.(it.id)}
                />
              ))}
            </div>
          )}
        </div>

        <div className="hidden lg:block">
          <div className="sticky top-6">{DetailsSlot}</div>
        </div>
      </div>
    </div>
  );
}

function ListingCard({
  it,
  selected,
  currentUserId,
  loading,
  requestState, // "none" | "pending" | "accepted" | "rejected"
  onOpen,
  onEdit,
  onDelete,
  onRequest,
  onLogin,
  onNext,
}) {
  const role = String(it.role || "").toLowerCase();
  const isTraveler = role === "traveler";

  const ownerId =
    it.owner_id ?? it.user_id ?? it.created_by ?? it.createdBy ?? null;

  const isAuthed = !!currentUserId;
  const isOwner = isAuthed && String(ownerId) === String(currentUserId);

  const status = String(it.status || "open").toLowerCase();
  const isOpen = status === "open";

  const fromLabel =
    it.from_label ||
    [it.from_city, it.from_country].filter(Boolean).join(", ") ||
    it.from ||
    "—";

  const toLabel =
    it.to_label ||
    [it.to_city, it.to_country].filter(Boolean).join(", ") ||
    it.to ||
    "—";

  const createdAt = it.created_at || it.createdAt || null;
  const travelDate = it.travel_date || it.arrival_date || it.date || null;

  const weightVal =
    it.available_weight ?? it.weight ?? it.max_weight ?? it.maxWeight ?? null;

  const weightText =
    weightVal === null || weightVal === undefined || weightVal === ""
      ? "—"
      : `${weightVal} kg`;

  const rewardVal =
    it.reward_amount ??
    it.reward ??
    it.reward_value ??
    it.price_value ??
    it.price ??
    null;

  const rewardNum = Number(rewardVal || 0) || 0;
  const currency = it.currency || "USD";

  const rewardText =
    rewardVal === null ||
    rewardVal === undefined ||
    String(rewardVal).trim() === ""
      ? "—"
      : `${currency} ${rewardVal}`;

  const hasReward = rewardNum > 0;

  const pct = Number(it.negotiation_pct ?? 0.15);
  const minOffer = hasReward
    ? Math.max(0, Math.floor(rewardNum * (1 - pct)))
    : null;
  const maxOffer = hasReward ? Math.ceil(rewardNum * (1 + pct)) : null;

  const itemTitle =
    it.item_title || it.item_type || it.item || it.title || "Item";
  const itemNotes =
    it.description || it.item_notes || it.notes || it.details || "";

  // ✅ Request opens modal (choose shipment + offer), so disable when pending/accepted
  const requestDisabled =
    loading ||
    isOwner ||
    (isAuthed && (requestState === "pending" || requestState === "accepted"));

  const primaryAction = !isAuthed
    ? { key: "login", label: "Login", icon: LogIn, onClick: onLogin }
    : isOwner
    ? { key: "owner", label: "Your trip", icon: Send, onClick: null }
    : requestState === "pending"
    ? { key: "pending", label: "Pending", icon: Hourglass, onClick: null }
    : requestState === "accepted"
    ? { key: "next", label: "Next step", icon: MessageCircle, onClick: onNext }
    : requestState === "rejected"
    ? { key: "rejected", label: "Try again", icon: XCircle, onClick: onRequest }
    : { key: "request", label: "Request", icon: Send, onClick: onRequest };

  const PrimaryIcon = primaryAction.icon;

  const primaryStyle =
    primaryAction.key === "pending"
      ? "border-amber-200 bg-amber-50 text-amber-800"
      : primaryAction.key === "accepted" || primaryAction.key === "next"
      ? "border-emerald-200 bg-emerald-600 text-white hover:bg-emerald-700"
      : primaryAction.key === "login"
      ? "border-indigo-200 bg-white text-indigo-700 hover:bg-indigo-50"
      : primaryAction.key === "owner"
      ? "border-slate-200 bg-white text-slate-400"
      : primaryAction.key === "rejected"
      ? "border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100"
      : "border-indigo-200 bg-indigo-600 text-white hover:bg-indigo-700";

  const primaryDisabled =
    loading || primaryAction.key === "owner" || primaryAction.key === "pending";

  return (
    <div
      className={cn(
        "group rounded-3xl border bg-white shadow-sm overflow-hidden",
        "transition-all hover:shadow-md",
        selected
          ? "border-indigo-300 ring-2 ring-indigo-100"
          : "border-slate-200"
      )}
    >
      {/* HEADER */}
      <div className="p-4 pb-3 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <span
              className={cn(
                "inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-extrabold ring-1",
                isTraveler
                  ? "bg-indigo-50 text-indigo-700 ring-indigo-100"
                  : "bg-emerald-50 text-emerald-700 ring-emerald-100"
              )}
            >
              {isTraveler ? <Plane size={14} /> : <SendHorizontal size={14} />}
              {isTraveler ? "Traveler" : "Sender"}
            </span>

            <span
              className={cn(
                "inline-flex items-center px-2.5 py-1 rounded-full text-xs font-extrabold ring-1",
                isOpen
                  ? "bg-emerald-50 text-emerald-700 ring-emerald-100"
                  : "bg-amber-50 text-amber-800 ring-amber-100"
              )}
            >
              {status}
            </span>

            {hasReward ? (
              <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-extrabold bg-indigo-600 text-white">
                <Sparkles size={14} />
                High reward
              </span>
            ) : null}
          </div>

          <div className="mt-2 flex items-center gap-2">
            <div className="text-base font-extrabold text-slate-900">
              Trip #{it.id}
            </div>
          </div>

          <div className="mt-1 text-xs text-slate-500 font-semibold flex flex-wrap items-center gap-2">
            <span className="inline-flex items-center gap-1">
              <Calendar size={12} />
              {createdAt || "—"}
            </span>
            <span className="text-slate-300">•</span>
            <span className="inline-flex items-center gap-1">
              <MapPin size={12} />
              Owner #{ownerId ?? "—"}
            </span>
          </div>
        </div>
      </div>

      {/* ROUTE */}
      <div className="px-4">
        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
          <div className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wide">
            Route
          </div>

          <div className="mt-2 grid grid-cols-[1fr_auto_1fr] items-center gap-2">
            <div className="min-w-0">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {fromLabel}
              </div>
              <div className="text-xs text-slate-500">From</div>
            </div>

            <div className="h-9 w-9 rounded-full bg-white border border-slate-200 flex items-center justify-center text-slate-700 font-black">
              →
            </div>

            <div className="min-w-0 text-right">
              <div className="text-sm font-extrabold text-slate-900 truncate">
                {toLabel}
              </div>
              <div className="text-xs text-slate-500">To</div>
            </div>
          </div>

          {it.from_airport || it.to_airport ? (
            <div className="mt-2 text-xs text-slate-500 font-semibold truncate">
              {it.from_airport ? `From: ${it.from_airport}` : ""}
              {it.from_airport && it.to_airport ? " • " : ""}
              {it.to_airport ? `To: ${it.to_airport}` : ""}
            </div>
          ) : null}
        </div>
      </div>

      {/* STATS */}
      <div className="px-4 mt-3">
        <div className="grid grid-cols-3 gap-2">
          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <DollarSign size={14} /> Reward
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900">
              {rewardText}
            </div>
            {hasReward && minOffer != null && maxOffer != null ? (
              <div className="mt-1 text-[11px] text-slate-500 font-semibold">
                Negotiable: {currency} {minOffer}–{maxOffer}
              </div>
            ) : null}
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Weight size={14} /> Weight
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900">
              {weightText}
            </div>
          </div>

          <div className="rounded-2xl border border-slate-200 bg-white p-3">
            <div className="flex items-center gap-1 text-[11px] font-extrabold text-slate-500 uppercase">
              <Calendar size={14} /> Date
            </div>
            <div className="mt-1 text-sm font-extrabold text-slate-900">
              {travelDate || "—"}
            </div>
          </div>
        </div>
      </div>

      {/* ITEM */}
      <div className="px-4 mt-3">
        <div className="rounded-2xl border border-slate-200 bg-white p-3">
          <div className="text-[11px] font-extrabold text-slate-500 uppercase tracking-wide">
            Item
          </div>

          <div className="mt-1 text-sm font-extrabold text-slate-900">
            {itemTitle}
          </div>

          {itemNotes ? (
            <div className="mt-1 text-sm text-slate-600 leading-6 line-clamp-2">
              {itemNotes}
            </div>
          ) : (
            <div className="mt-1 text-sm text-slate-400">No details.</div>
          )}
        </div>
      </div>

      {/* ACTIONS */}
      <div className="p-4 pt-3 border-t border-slate-100 bg-slate-50/40">
        <div className="grid grid-cols-2 gap-2">
          <button
            type="button"
            onClick={onOpen}
            className="inline-flex items-center justify-center gap-2 px-4 py-3 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold"
          >
            View details
            <ArrowRight size={16} />
          </button>

          <button
            type="button"
            disabled={primaryDisabled || requestDisabled}
            onClick={primaryAction.onClick || undefined}
            className={cn(
              "inline-flex items-center justify-center gap-2 px-4 py-3 rounded-2xl text-sm font-extrabold border",
              primaryStyle,
              primaryDisabled || requestDisabled ? "opacity-80" : ""
            )}
            title={
              !isAuthed
                ? "Login first"
                : isOwner
                ? "Your trip"
                : requestState === "pending"
                ? "Request sent"
                : requestState === "accepted"
                ? "Proceed to next step"
                : requestState === "rejected"
                ? "Rejected"
                : "Send request"
            }
          >
            <PrimaryIcon size={16} />
            {primaryAction.label}
          </button>
        </div>

        {!isOwner && isAuthed && requestState === "pending" ? (
          <div className="mt-2 text-xs font-semibold text-amber-700 flex items-center gap-2">
            <Hourglass size={14} />
            Request sent — waiting for approval.
          </div>
        ) : null}

        {!isOwner && isAuthed && requestState === "accepted" ? (
          <div className="mt-2 text-xs font-semibold text-emerald-700 flex items-center gap-2">
            <CheckCircle2 size={14} />
            Accepted — open details to continue.
          </div>
        ) : null}

        {!isOwner && isAuthed && requestState === "rejected" ? (
          <div className="mt-2 text-xs font-semibold text-rose-700 flex items-center gap-2">
            <XCircle size={14} />
            Rejected — you can try again.
          </div>
        ) : null}

        {isOwner ? (
          <div className="mt-2 flex items-center gap-2">
            <button
              type="button"
              onClick={onEdit}
              className="flex-1 inline-flex items-center justify-center px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 text-sm font-extrabold"
            >
              Edit
            </button>

            <button
              type="button"
              onClick={onDelete}
              className="flex-1 inline-flex items-center justify-center px-4 py-2.5 rounded-2xl border border-red-200 bg-white hover:bg-red-50 text-sm font-extrabold text-red-700"
            >
              Delete
            </button>
          </div>
        ) : null}
      </div>
    </div>
  );
}
