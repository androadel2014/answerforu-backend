// src/components/carry/CarryView.jsx
import React, { useEffect, useMemo, useState } from "react";
import toast from "react-hot-toast";
import { Package, Plus, RefreshCw, Lock, Trash2, Pencil } from "lucide-react";
import { toastConfirm } from "../../lib/notify";
import { useLocation, useNavigate } from "react-router-dom";

import { cn, emptyForm, getCurrentUserId } from "./shared/carryUtils";

import {
  fetchCarryListings,
  fetchCarryDetails,
  upsertCarryListing,
  deleteCarryListing as apiDelete,
  requestCarryMatch,
  sendCarryMessage,
  fetchCarryShipments,
  createCarryShipment,
  updateCarryShipment,
  deleteCarryShipment,
} from "./shared/carryApi";

import ExploreSection from "./sections/ExploreSection";
import CreateForm from "./sections/CreateForm";
import DetailsPanel from "./sections/DetailsPanel";
import { FiltersDrawer, MobileDetailsDrawer } from "./sections/Overlays";

/* =========================
   Auth helpers
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

const TABS2 = [
  { key: "explore", label: "Trips" },
  { key: "create", label: "New Trip" },
  { key: "shipments", label: "My Shipments" },
];

export default function CarryView() {
  const [tab, setTab] = useState("explore");
  const location = useLocation();
  const navigate = useNavigate();

  const [loading, setLoading] = useState(false);
  const [items, setItems] = useState([]);

  // ✅ current user id MUST be reactive
  const [currentUserId, setCurrentUserId] = useState(getCurrentUserId());
  useEffect(() => {
    const sync = () => setCurrentUserId(getCurrentUserId());
    window.addEventListener("storage", sync);
    window.addEventListener("focus", sync);
    return () => {
      window.removeEventListener("storage", sync);
      window.removeEventListener("focus", sync);
    };
  }, []);

  // filters
  const [q, setQ] = useState("");
  const [roleFilter, setRoleFilter] = useState("traveler"); // ✅ Trips only by default
  const [fromCountry, setFromCountry] = useState("");
  const [toCountry, setToCountry] = useState("");
  const [minWeight, setMinWeight] = useState("");
  const [maxReward, setMaxReward] = useState("");
  const [filtersOpen, setFiltersOpen] = useState(false);

  // create trip
  const [form, setForm] = useState({ ...emptyForm, role: "traveler" });
  const [editId, setEditId] = useState(null);

  // details + chat (legacy inline panel)
  const [details, setDetails] = useState(null);
  const [msg, setMsg] = useState("");
  const [messages, setMessages] = useState([]);
  const [detailsOpenMobile, setDetailsOpenMobile] = useState(false);

  // shipments (sender)
  const [shipLoading, setShipLoading] = useState(false);
  const [shipments, setShipments] = useState([]);
  const [shipEditId, setShipEditId] = useState(null);
  const [shipForm, setShipForm] = useState({
    from_country: "",
    from_city: "",
    to_country: "",
    to_city: "",
    deadline: "",
    item_title: "",
    item_desc: "",
    item_weight: "",
    budget_amount: "",
    budget_currency: "USD",
  });

  // request modal (trip -> choose shipment + offer)
  const [reqModalOpen, setReqModalOpen] = useState(false);
  const [reqTripId, setReqTripId] = useState(null);
  const [reqShipmentId, setReqShipmentId] = useState("");
  const [reqOfferAmount, setReqOfferAmount] = useState("");
  const [reqOfferCurrency, setReqOfferCurrency] = useState("USD");
  const [reqNote, setReqNote] = useState("");

  const selected = details?.item || null;

  function goLogin(nextPath) {
    navigate("/auth", { state: { next: nextPath || location.pathname } });
  }

  function requireAuth(nextPath) {
    if (isAuthed()) return true;
    toast.error("Please login first.");
    goLogin(nextPath);
    return false;
  }

  function isOwnerOf(it) {
    const ownerId =
      it?.owner_id ?? it?.user_id ?? it?.created_by ?? it?.createdBy ?? null;
    return currentUserId && String(ownerId) === String(currentUserId);
  }

  async function loadListings() {
    setLoading(true);
    try {
      const r = await fetchCarryListings({
        q,
        role: "traveler", // ✅ FORCE trips only
        from_country: fromCountry,
        to_country: toCountry,
      });
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const root = r?.data ?? r;
      let arr = Array.isArray(root?.items)
        ? root.items
        : Array.isArray(root?.listings)
        ? root.listings
        : Array.isArray(root?.rows)
        ? root.rows
        : Array.isArray(root)
        ? root
        : [];

      const minW = minWeight === "" ? null : Number(minWeight);
      const maxR = maxReward === "" ? null : Number(maxReward);

      if (Number.isFinite(minW))
        arr = arr.filter((x) => (x.available_weight ?? 0) >= minW);
      if (Number.isFinite(maxR))
        arr = arr.filter((x) => (x.reward_amount ?? 0) <= maxR);

      setItems(arr);
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function loadShipments() {
    if (!requireAuth("/carry?tab=shipments")) return;
    setShipLoading(true);
    try {
      const r = await fetchCarryShipments();
      if (!r?.ok) throw new Error(r?.error || "Failed");
      const root = r?.data ?? r;
      const arr = Array.isArray(root?.items)
        ? root.items
        : Array.isArray(root)
        ? root
        : [];
      setShipments(arr);
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setShipLoading(false);
    }
  }

  // (legacy inline details; not used by cards now, but kept)
  async function openDetails(id) {
    setLoading(true);
    try {
      const r = await fetchCarryDetails(id);
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const root = r?.data ?? r;
      setDetails(root);
      setEditId(null);

      setMessages(Array.isArray(root?.messages) ? root.messages : []);

      setTab("explore");
      setDetailsOpenMobile(true);
      window.scrollTo({ top: 0, behavior: "smooth" });
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function submitListing() {
    if (!requireAuth("/carry?tab=create")) return;

    try {
      setLoading(true);

      const payload = {
        ...form,
        role: "traveler",
        currency: "USD",
        available_weight:
          form.available_weight === "" ? null : Number(form.available_weight),
        reward_amount:
          form.reward_amount === "" ? null : Number(form.reward_amount),
      };

      const r = await upsertCarryListing({ payload, editId });
      if (!r?.ok) throw new Error(r?.error || "Failed");

      toast.success(editId ? "Updated ✅" : "Created ✅");
      setForm({ ...emptyForm, role: "traveler" });
      setEditId(null);

      await loadListings();
      setTab("explore");
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  async function deleteListing(id) {
    if (!requireAuth(location.pathname)) return;

    const it = items.find((x) => Number(x.id) === Number(id));
    if (!isOwnerOf(it)) {
      toast.error("You can only delete your own trip.");
      return;
    }

    const ok = await toastConfirm("Delete this trip?");
    if (!ok) return;

    try {
      setLoading(true);
      const r = await apiDelete(id);
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success("Deleted ✅");

      if (details?.item?.id && Number(details.item.id) === Number(id)) {
        setDetails(null);
        setDetailsOpenMobile(false);
      }

      await loadListings();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setLoading(false);
    }
  }

  // =========================
  // SHIPMENTS UI actions
  // =========================
  async function submitShipment() {
    if (!requireAuth("/carry?tab=shipments")) return;

    const payload = {
      ...shipForm,
      item_weight:
        shipForm.item_weight === "" ? null : Number(shipForm.item_weight),
      budget_amount:
        shipForm.budget_amount === "" ? null : Number(shipForm.budget_amount),
      budget_currency: shipForm.budget_currency || "USD",
    };

    try {
      setShipLoading(true);
      const r = shipEditId
        ? await updateCarryShipment(shipEditId, payload)
        : await createCarryShipment(payload);

      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success(shipEditId ? "Shipment updated ✅" : "Shipment created ✅");

      setShipEditId(null);
      setShipForm({
        from_country: "",
        from_city: "",
        to_country: "",
        to_city: "",
        deadline: "",
        item_title: "",
        item_desc: "",
        item_weight: "",
        budget_amount: "",
        budget_currency: "USD",
      });

      await loadShipments();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setShipLoading(false);
    }
  }

  async function onDeleteShipment(id) {
    if (!requireAuth("/carry?tab=shipments")) return;
    const ok = await toastConfirm("Delete this shipment?");
    if (!ok) return;

    try {
      setShipLoading(true);
      const r = await deleteCarryShipment(id);
      if (!r?.ok) throw new Error(r?.error || "Failed");
      toast.success("Deleted ✅");
      await loadShipments();
    } catch (e) {
      toast.error(String(e?.message || e));
    } finally {
      setShipLoading(false);
    }
  }

  function onEditShipment(it) {
    setShipEditId(it.id);
    setShipForm({
      from_country: it.from_country || "",
      from_city: it.from_city || "",
      to_country: it.to_country || "",
      to_city: it.to_city || "",
      deadline: it.deadline || "",
      item_title: it.item_title || "",
      item_desc: it.item_desc || "",
      item_weight:
        it.item_weight === null || it.item_weight === undefined
          ? ""
          : String(it.item_weight),
      budget_amount:
        it.budget_amount === null || it.budget_amount === undefined
          ? ""
          : String(it.budget_amount),
      budget_currency: it.budget_currency || "USD",
    });
    setTab("shipments");
    window.scrollTo({ top: 0, behavior: "smooth" });
  }

  // =========================
  // REQUEST FLOW: Trip -> choose shipment + offer
  // =========================
  async function openRequestModal(tripId) {
    if (!requireAuth(`/carry/listings/${tripId}`)) return;

    // must have shipments
    if (!shipments?.length) {
      await loadShipments();
    }
    if (!shipments?.length) {
      toast.error("Create a shipment first (My Shipments).");
      setTab("shipments");
      return;
    }

    setReqTripId(tripId);
    setReqShipmentId(String(shipments[0]?.id || ""));
    setReqOfferAmount("");
    setReqOfferCurrency("USD");
    setReqNote("");
    setReqModalOpen(true);
  }

  async function submitRequestModal() {
    if (!reqTripId) return;

    const shipment_id = Number(reqShipmentId);
    if (!shipment_id) {
      toast.error("Choose a shipment.");
      return;
    }
    const offer_amount = reqOfferAmount === "" ? null : Number(reqOfferAmount);
    if (!Number.isFinite(offer_amount) || offer_amount <= 0) {
      toast.error("Enter a valid offer amount.");
      return;
    }

    // optimistic pending on card
    setItems((prev) =>
      (prev || []).map((x) =>
        Number(x.id) === Number(reqTripId)
          ? {
              ...x,
              my_request_status: "pending",
              myRequestStatus: "pending",
              request_status: "pending",
              requestStatus: "pending",
            }
          : x
      )
    );

    try {
      setLoading(true);
      const r = await requestCarryMatch(reqTripId, {
        shipment_id,
        offer_amount,
        offer_currency: reqOfferCurrency || "USD",
        note: reqNote || "",
      });
      if (!r?.ok) throw new Error(r?.error || r?.data?.error || "Failed");

      toast.success("Request sent ✅");
      setReqModalOpen(false);

      // refresh details if open
      const fresh = await fetchCarryDetails(reqTripId);
      if (fresh?.ok) {
        const d = fresh?.data ?? fresh;
        setDetails(d);
        setMessages(Array.isArray(d?.messages) ? d.messages : []);
      }

      return { status: "pending" };
    } catch (e) {
      // revert optimistic
      setItems((prev) =>
        (prev || []).map((x) =>
          Number(x.id) === Number(reqTripId)
            ? {
                ...x,
                my_request_status: null,
                myRequestStatus: null,
                request_status: null,
                requestStatus: null,
              }
            : x
        )
      );
      toast.error(String(e?.message || e));
      return null;
    } finally {
      setLoading(false);
    }
  }

  async function sendMessage(listingId) {
    if (!requireAuth(`/carry/listings/${listingId}`)) return;

    const text = msg.trim();
    if (!text) return;
    setMsg("");

    try {
      const r = await sendCarryMessage(listingId, text);
      if (!r?.ok) throw new Error(r?.error || "Failed");

      const fresh = await fetchCarryDetails(listingId);
      if (fresh?.ok) {
        const root = fresh?.data ?? fresh;
        setDetails(root);
        setMessages(Array.isArray(root?.messages) ? root.messages : []);
      }
    } catch (e) {
      toast.error(String(e?.message || e));
    }
  }

  useEffect(() => {
    loadListings();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    const sp = new URLSearchParams(location.search || "");
    const tabQ = sp.get("tab");
    const editQ = sp.get("edit");

    if (tabQ && tabQ !== tab) setTab(tabQ);

    if (editQ) {
      if (!requireAuth("/carry?tab=create")) return;

      const id = Number(editQ);
      const it = items.find((x) => Number(x.id) === id);

      if (it) {
        setEditId(it.id);
        setForm((s) => ({
          ...s,
          ...emptyForm,
          ...it,
          role: "traveler",
          currency: it.currency || "USD",
        }));
        setTab("create");
        window.scrollTo({ top: 0, behavior: "smooth" });

        sp.delete("edit");
        sp.delete("tab");
        const next = sp.toString();
        navigate(`/carry${next ? `?${next}` : ""}`, { replace: true });
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [location.search, items]);

  useEffect(() => {
    function onUpdated(e) {
      const next = e?.detail?.details;
      if (!next?.item?.id) return;
      setDetails(next);
      setMessages(Array.isArray(next?.messages) ? next.messages : []);
    }
    window.addEventListener("carry:detailsUpdated", onUpdated);
    return () => window.removeEventListener("carry:detailsUpdated", onUpdated);
  }, []);

  const activeCount = useMemo(() => items.length, [items]);
  const headerStats = useMemo(() => {
    const travelers = items.filter((x) => x.role === "traveler").length;
    return { travelers };
  }, [items]);

  return (
    <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6">
      <div className="rounded-3xl overflow-hidden border border-slate-200 bg-white shadow-sm">
        {/* HERO */}
        <div className="relative p-6 sm:p-8 bg-gradient-to-r from-slate-950 via-slate-900 to-indigo-950 text-white">
          <div className="flex items-start justify-between gap-4">
            <div className="flex items-center gap-3">
              <div className="w-11 h-11 rounded-2xl bg-white/10 border border-white/15 flex items-center justify-center">
                <Package size={22} />
              </div>
              <div>
                <div className="text-2xl sm:text-3xl font-black tracking-tight">
                  Carry
                </div>
                <div className="text-sm text-white/75 mt-1">
                  Trips (Travelers) • Shipments (Senders) • Match then chat
                </div>
              </div>
            </div>

            <div className="hidden sm:flex items-center gap-2">
              <button
                disabled={loading}
                onClick={loadListings}
                className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold flex items-center gap-2 disabled:opacity-60"
              >
                <RefreshCw size={16} />
                Refresh
              </button>

              <button
                onClick={() => {
                  if (!requireAuth("/carry?tab=create")) return;
                  setTab("create");
                }}
                className="px-4 py-2 rounded-2xl bg-white text-slate-900 hover:bg-slate-100 text-sm font-extrabold flex items-center gap-2"
              >
                <Plus size={16} />
                New Trip
              </button>

              {!isAuthed() ? (
                <button
                  onClick={() => goLogin("/carry")}
                  className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold flex items-center gap-2"
                >
                  <Lock size={16} />
                  Login
                </button>
              ) : null}
            </div>
          </div>

          <div className="mt-6 grid grid-cols-2 gap-3 max-w-md">
            <StatCard label="Trips" value={activeCount} />
            <StatCard label="Travelers" value={headerStats.travelers} />
          </div>

          <div className="mt-6 flex gap-2">
            {TABS2.map((t) => (
              <button
                key={t.key}
                onClick={() => {
                  if (t.key === "create" || t.key === "shipments") {
                    if (!requireAuth(`/carry?tab=${t.key}`)) return;
                  }
                  setTab(t.key);
                  if (t.key === "shipments") loadShipments();
                }}
                className={cn(
                  "px-4 py-2 rounded-2xl text-sm font-extrabold border transition-all",
                  tab === t.key
                    ? "bg-white text-slate-900 border-white"
                    : "bg-white/10 text-white border-white/15 hover:bg-white/15"
                )}
              >
                {t.label}
              </button>
            ))}
          </div>
        </div>

        {/* BODY */}
        {tab === "explore" ? (
          <ExploreSection
            loading={loading}
            items={items} // trips only
            selectedId={selected?.id}
            q={q}
            setQ={setQ}
            roleFilter={roleFilter}
            setRoleFilter={() => {}} // disabled (Trips only)
            onOpenFilters={() => setFiltersOpen(true)}
            onSearch={loadListings}
            onOpenDetails={(id) => navigate(`/carry/listings/${id}`)}
            onGoLogin={() => goLogin("/carry")}
            // ✅ request now opens modal to choose shipment + offer
            onRequestFromCard={(id) => openRequestModal(id)}
            currentUserId={currentUserId}
            onEditFromCard={(it) => {
              if (!requireAuth("/carry?tab=create")) return;

              setEditId(it.id);
              setForm((s) => ({
                ...s,
                ...emptyForm,
                ...it,
                role: "traveler",
                currency: it.currency || "USD",
              }));
              setTab("create");
              window.scrollTo({ top: 0, behavior: "smooth" });
            }}
            onDeleteFromCard={(it) => deleteListing(it.id)}
            DetailsSlot={
              !details?.item ? (
                <div className="rounded-3xl border border-slate-200 bg-white p-5">
                  <div className="text-sm font-black text-slate-900">
                    Select a trip
                  </div>
                  <div className="text-sm text-slate-500 mt-1">
                    Open any trip to see details & request flow.
                  </div>
                </div>
              ) : (
                <DetailsPanel
                  loading={loading}
                  details={details}
                  messages={messages}
                  msg={msg}
                  setMsg={setMsg}
                  currentUserId={currentUserId}
                  onSend={() => sendMessage(details.item.id)}
                  // ✅ request from panel also opens modal
                  onRequest={() => openRequestModal(details.item.id)}
                  onEdit={() => {
                    if (!requireAuth("/carry?tab=create")) return;

                    const it = details?.item;
                    if (!it) return;
                    setEditId(it.id);
                    setForm((s) => ({
                      ...s,
                      ...emptyForm,
                      ...it,
                      role: "traveler",
                      currency: it.currency || "USD",
                    }));
                    setTab("create");
                    window.scrollTo({ top: 0, behavior: "smooth" });
                  }}
                  onDelete={
                    isOwnerOf(details?.item)
                      ? () => deleteListing(details.item.id)
                      : undefined
                  }
                />
              )
            }
          />
        ) : null}

        {tab === "create" ? (
          <CreateForm
            loading={loading}
            editId={editId}
            form={form}
            setForm={setForm}
            onSubmit={submitListing}
            onCancelEdit={() => {
              setEditId(null);
              setForm({ ...emptyForm, role: "traveler" });
            }}
          />
        ) : null}

        {tab === "shipments" ? (
          <div className="p-4 sm:p-6">
            <div className="max-w-5xl">
              <div className="rounded-3xl border border-slate-200 bg-white shadow-sm overflow-hidden">
                <div className="p-6 bg-slate-950 text-white">
                  <div className="flex items-center justify-between gap-3">
                    <div>
                      <div className="text-2xl font-black tracking-tight">
                        {shipEditId
                          ? `Edit Shipment #${shipEditId}`
                          : "My Shipments"}
                      </div>
                      <div className="text-sm text-white/75 mt-1">
                        Create shipments (what you want delivered). Then request
                        a trip.
                      </div>
                    </div>

                    <button
                      disabled={shipLoading}
                      onClick={loadShipments}
                      className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold disabled:opacity-60 inline-flex items-center gap-2"
                    >
                      <RefreshCw size={16} />
                      Refresh
                    </button>
                  </div>
                </div>

                <div className="p-6 grid lg:grid-cols-2 gap-6">
                  {/* FORM */}
                  <div className="space-y-3">
                    <div className="text-sm font-black text-slate-900">
                      Shipment details
                    </div>

                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="From country">
                        <input
                          value={shipForm.from_country}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              from_country: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>

                      <Field label="From city">
                        <input
                          value={shipForm.from_city}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              from_city: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>
                    </div>

                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="To country">
                        <input
                          value={shipForm.to_country}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              to_country: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>

                      <Field label="To city">
                        <input
                          value={shipForm.to_city}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              to_city: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>
                    </div>

                    <Field label="Item title">
                      <input
                        value={shipForm.item_title}
                        onChange={(e) =>
                          setShipForm((s) => ({
                            ...s,
                            item_title: e.target.value,
                          }))
                        }
                        className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        placeholder="e.g. iPhone 15"
                      />
                    </Field>

                    <Field label="Item description">
                      <textarea
                        value={shipForm.item_desc}
                        onChange={(e) =>
                          setShipForm((s) => ({
                            ...s,
                            item_desc: e.target.value,
                          }))
                        }
                        className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white min-h-[120px]"
                        placeholder="Size, color, notes…"
                      />
                    </Field>

                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="Item weight (kg)">
                        <input
                          type="number"
                          value={shipForm.item_weight}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              item_weight: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>

                      <Field label="Deadline">
                        <input
                          type="datetime-local"
                          value={shipForm.deadline}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              deadline: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                        />
                      </Field>
                    </div>

                    <div className="grid sm:grid-cols-2 gap-3">
                      <Field label="Budget currency">
                        <select
                          value={shipForm.budget_currency}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              budget_currency: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                        >
                          <option value="USD">USD</option>
                          <option value="EUR">EUR</option>
                          <option value="EGP">EGP</option>
                          <option value="GBP">GBP</option>
                        </select>
                      </Field>

                      <Field label="Budget amount">
                        <input
                          type="number"
                          value={shipForm.budget_amount}
                          onChange={(e) =>
                            setShipForm((s) => ({
                              ...s,
                              budget_amount: e.target.value,
                            }))
                          }
                          className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                          placeholder="e.g. 50"
                        />
                      </Field>
                    </div>

                    <div className="flex gap-2 pt-2">
                      {shipEditId ? (
                        <button
                          disabled={shipLoading}
                          onClick={() => {
                            setShipEditId(null);
                            setShipForm({
                              from_country: "",
                              from_city: "",
                              to_country: "",
                              to_city: "",
                              deadline: "",
                              item_title: "",
                              item_desc: "",
                              item_weight: "",
                              budget_amount: "",
                              budget_currency: "USD",
                            });
                          }}
                          className="flex-1 px-4 py-3 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold disabled:opacity-60"
                        >
                          Cancel edit
                        </button>
                      ) : null}

                      <button
                        disabled={shipLoading}
                        onClick={submitShipment}
                        className="flex-1 px-4 py-3 rounded-2xl bg-indigo-600 text-white hover:bg-indigo-700 font-extrabold disabled:opacity-60"
                      >
                        {shipEditId ? "Update Shipment" : "Create Shipment"}
                      </button>
                    </div>
                  </div>

                  {/* LIST */}
                  <div className="space-y-3">
                    <div className="text-sm font-black text-slate-900">
                      Your shipments
                    </div>

                    <div className="rounded-3xl border border-slate-200 bg-slate-50 p-3 max-h-[620px] overflow-auto space-y-2">
                      {shipLoading ? (
                        <div className="text-sm text-slate-500 p-2">
                          Loading…
                        </div>
                      ) : shipments?.length ? (
                        shipments.map((s) => (
                          <div
                            key={s.id}
                            className="rounded-2xl border border-slate-200 bg-white p-3"
                          >
                            <div className="flex items-start justify-between gap-2">
                              <div className="min-w-0">
                                <div className="font-extrabold text-slate-900 truncate">
                                  #{s.id} • {s.item_title || "Shipment"}
                                </div>
                                <div className="text-xs text-slate-500 mt-1">
                                  {[
                                    [s.from_city, s.from_country]
                                      .filter(Boolean)
                                      .join(", "),
                                    "→",
                                    [s.to_city, s.to_country]
                                      .filter(Boolean)
                                      .join(", "),
                                  ].join(" ")}
                                </div>
                                <div className="text-xs text-slate-500 mt-1">
                                  Weight: {s.item_weight ?? "—"} kg • Budget:{" "}
                                  {s.budget_currency || "USD"}{" "}
                                  {s.budget_amount ?? "—"}
                                </div>
                              </div>

                              <div className="flex items-center gap-2">
                                <button
                                  onClick={() => onEditShipment(s)}
                                  className="p-2 rounded-xl border border-slate-200 bg-white hover:bg-slate-50"
                                  title="Edit"
                                >
                                  <Pencil size={16} />
                                </button>
                                <button
                                  onClick={() => onDeleteShipment(s.id)}
                                  className="p-2 rounded-xl border border-red-200 bg-white hover:bg-red-50 text-red-700"
                                  title="Delete"
                                >
                                  <Trash2 size={16} />
                                </button>
                              </div>
                            </div>
                          </div>
                        ))
                      ) : (
                        <div className="text-sm text-slate-500 p-2">
                          No shipments yet. Create one on the left.
                        </div>
                      )}
                    </div>

                    <div className="text-xs text-slate-500 font-bold">
                      After creating a shipment, go to Trips and request a trip.
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : null}
      </div>

      <FiltersDrawer
        open={filtersOpen}
        onClose={() => setFiltersOpen(false)}
        fromCountry={fromCountry}
        setFromCountry={setFromCountry}
        toCountry={toCountry}
        setToCountry={setToCountry}
        minWeight={minWeight}
        setMinWeight={setMinWeight}
        maxReward={maxReward}
        setMaxReward={setMaxReward}
        loading={loading}
        onReset={() => {
          setFromCountry("");
          setToCountry("");
          setMinWeight("");
          setMaxReward("");
        }}
        onApply={async () => {
          setFiltersOpen(false);
          await loadListings();
        }}
      />

      <MobileDetailsDrawer
        open={detailsOpenMobile}
        onClose={() => setDetailsOpenMobile(false)}
        details={details}
      >
        <DetailsPanel
          loading={loading}
          details={details}
          messages={messages}
          msg={msg}
          setMsg={setMsg}
          currentUserId={currentUserId}
          onSend={() => sendMessage(details.item.id)}
          onRequest={() => openRequestModal(details.item.id)}
          onEdit={() => {
            if (!requireAuth("/carry?tab=create")) return;

            const it = details?.item;
            if (!it) return;
            setEditId(it.id);
            setForm((s) => ({
              ...s,
              ...emptyForm,
              ...it,
              role: "traveler",
              currency: it.currency || "USD",
            }));
            setTab("create");
            window.scrollTo({ top: 0, behavior: "smooth" });
          }}
          onDelete={
            isOwnerOf(details?.item)
              ? () => deleteListing(details.item.id)
              : undefined
          }
        />
      </MobileDetailsDrawer>

      {/* =====================
          REQUEST MODAL (Trip -> Shipment + Offer)
         ===================== */}
      {reqModalOpen ? (
        <div className="fixed inset-0 z-[90] flex items-center justify-center p-4">
          <div
            className="absolute inset-0 bg-black/40"
            onClick={() => setReqModalOpen(false)}
          />
          <div className="relative w-full max-w-lg rounded-3xl border bg-white shadow-xl overflow-hidden">
            <div className="px-5 py-4 border-b flex items-center justify-between">
              <div className="font-black text-slate-900">
                Send Request (Trip #{reqTripId})
              </div>
              <button
                className="px-2 py-1 rounded-xl hover:bg-slate-100 font-extrabold"
                onClick={() => setReqModalOpen(false)}
              >
                ✕
              </button>
            </div>

            <div className="p-5 space-y-4">
              <div className="text-sm text-slate-600">
                Choose one of your shipments, set your offer, then send request.
                Chat opens after acceptance.
              </div>

              <div>
                <div className="text-xs font-extrabold text-slate-500 uppercase">
                  Shipment
                </div>
                <select
                  value={reqShipmentId}
                  onChange={(e) => setReqShipmentId(e.target.value)}
                  className="mt-2 w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                >
                  {shipments.map((s) => (
                    <option key={s.id} value={String(s.id)}>
                      #{s.id} • {s.item_title || "Shipment"} •{" "}
                      {s.budget_currency || "USD"} {s.budget_amount ?? "—"}
                    </option>
                  ))}
                </select>
              </div>

              <div className="grid grid-cols-3 gap-2">
                <div>
                  <div className="text-xs font-extrabold text-slate-500 uppercase">
                    Currency
                  </div>
                  <select
                    value={reqOfferCurrency}
                    onChange={(e) => setReqOfferCurrency(e.target.value)}
                    className="mt-2 w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                  >
                    <option value="USD">USD</option>
                    <option value="EUR">EUR</option>
                    <option value="EGP">EGP</option>
                    <option value="GBP">GBP</option>
                  </select>
                </div>

                <div className="col-span-2">
                  <div className="text-xs font-extrabold text-slate-500 uppercase">
                    Offer amount
                  </div>
                  <input
                    type="number"
                    value={reqOfferAmount}
                    onChange={(e) => setReqOfferAmount(e.target.value)}
                    className="mt-2 w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                    placeholder="e.g. 50"
                  />
                </div>
              </div>

              <div>
                <div className="text-xs font-extrabold text-slate-500 uppercase">
                  Note (optional)
                </div>
                <textarea
                  value={reqNote}
                  onChange={(e) => setReqNote(e.target.value)}
                  className="mt-2 w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white min-h-[120px]"
                  placeholder="Delivery/meeting notes…"
                />
              </div>

              <div className="flex gap-2 pt-1">
                <button
                  onClick={() => setReqModalOpen(false)}
                  className="flex-1 px-4 py-3 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold"
                >
                  Cancel
                </button>
                <button
                  disabled={loading}
                  onClick={submitRequestModal}
                  className="flex-1 px-4 py-3 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold disabled:opacity-60"
                >
                  Send Request
                </button>
              </div>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  );
}

function StatCard({ label, value }) {
  return (
    <div className="rounded-2xl bg-white/10 border border-white/15 p-3">
      <div className="text-[11px] text-white/70 font-bold">{label}</div>
      <div className="text-lg font-black">{value}</div>
    </div>
  );
}

// small local Field (to avoid adding new file)
function Field({ label, children }) {
  return (
    <div>
      <div className="text-xs font-extrabold text-slate-600">{label}</div>
      <div className="mt-2">{children}</div>
    </div>
  );
}
