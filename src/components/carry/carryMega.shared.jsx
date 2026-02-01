// src/components/carry/carryMega.shared.jsx
import React from "react";
import { CarryCard, Pill } from "./card";
import { Lock, LogIn, Plus, Plane, Package, Search, Heart } from "lucide-react";

/* ====== Trip lock helpers (needs getTripLockCount) ====== */
export function getTripLockCount(it) {
  const n = it || {};
  const v =
    n.requests_count ??
    n.requestsCount ??
    n.requests_total ??
    n.requestsTotal ??
    n.matches_count ??
    n.matchesCount ??
    n.matches_total ??
    n.matchesTotal ??
    n.shipments_count ??
    n.shipmentsCount ??
    n.shipments_total ??
    n.shipmentsTotal ??
    null;

  const num = Number(v);
  if (Number.isFinite(num) && num > 0) return num;

  const arrLen =
    (Array.isArray(n.requests) ? n.requests.length : 0) ||
    (Array.isArray(n.matches) ? n.matches.length : 0) ||
    (Array.isArray(n.shipments) ? n.shipments.length : 0);

  return arrLen || 0;
}

export function isTripLocked(it) {
  const n = it || {};

  // explicit flags
  if (n.locked === true || n.is_locked === true || n.isLocked === true)
    return true;
  if (n.has_requests === true || n.hasRequests === true) return true;

  // counts
  if (getTripLockCount(n) > 0) return true;

  const approved =
    n.is_approved === true ||
    n.isApproved === true ||
    !!n.approved_at ||
    !!n.approvedAt;

  const st = String(n.status ?? n.trip_status ?? n.tripStatus ?? "")
    .trim()
    .toLowerCase();

  if (approved || ["approved", "published", "live"].includes(st)) return true;
  if (["matched", "in_progress", "completed"].includes(st)) return true;

  return false;
}

/* ====== Shipment lock helper ====== */
export function isShipmentLocked(it) {
  const n = it || {};
  const st = String(n.status ?? n.shipment_status ?? n.shipmentStatus ?? "")
    .trim()
    .toLowerCase();
  if (["matched", "in_transit", "delivered", "completed"].includes(st))
    return true;
  if (["cancelled", "canceled"].includes(st)) return true;
  return false;
}

/* ====== Normalizers ====== */
export function normalizeTrip(raw) {
  const it = raw || {};
  const ownerId =
    it.owner_id ?? it.user_id ?? it.created_by ?? it.createdBy ?? null;

  const airline =
    it.airline ??
    it.traveler_airline ??
    it.airline_name ??
    it.airlineName ??
    "";

  const flight =
    it.flight_number ??
    it.flightNumber ??
    it.traveler_flight_number ??
    it.flight_no ??
    it.flightNo ??
    "";

  const fromCode =
    it.from_airport_code ||
    it.from_airport ||
    it.from_iata ||
    it.from_code ||
    "";

  const toCode =
    it.to_airport_code || it.to_airport || it.to_iata || it.to_code || "";

  return {
    ...it,
    _type: "trip",
    _ownerId: ownerId ? Number(ownerId) || ownerId : ownerId,

    from_airport_code: String(fromCode || "").toUpperCase(),
    to_airport_code: String(toCode || "").toUpperCase(),

    travel_date: it.travel_date || it.date || "",
    arrival_date: it.arrival_date || "",

    available_weight: it.available_weight ?? it.availableKg ?? it.kg ?? null,

    airline: airline || "",
    flight_number: flight || "",

    meet_pref: it.meet_pref || "airport",
    meet_place: it.meet_place || it.meet_location || "",

    passport_image: it.passport_image || it.passportPhoto || "",
    visa_image: it.visa_image || it.visaPhoto || "",

    no_carry: it.no_carry || it.noCarry || it.restrictions || [],
    description: it.description || "",
    status: it.status || "open",
  };
}

export function normalizeShipment(raw) {
  const it = raw || {};
  const ownerId =
    it.owner_id ?? it.user_id ?? it.created_by ?? it.createdBy ?? null;

  return {
    ...it,
    _type: "shipment",
    _ownerId: ownerId ? Number(ownerId) || ownerId : ownerId,

    status: it.status || "open",
    is_active: it.is_active ?? it.isActive ?? 1,
    created_at: it.created_at || it.createdAt || null,
    updated_at: it.updated_at || it.updatedAt || null,

    category: it.category || it.item_category || it.cat || "other",

    from_country: it.from_country || "",
    to_country: it.to_country || "",
    from_city: it.from_city || "",
    to_city: it.to_city || "",

    deadline: it.deadline || it.deliver_before || "",
    item_title: it.item_title || it.title || "Shipment",
    item_desc: it.item_desc || it.description || "",
    item_weight: it.item_weight ?? it.weight ?? null,

    budget_amount: it.budget_amount ?? it.budget ?? null,
    budget_currency: it.budget_currency || it.currency || "USD",

    product_url: it.product_url || it.productUrl || it.url || "",
    image: it.item_image || it.image || it.photo || "",
  };
}

/* ====== Empty / MiniRow ====== */
export function EmptyState({ icon: Icon, title, desc, loading }) {
  return (
    <div className="rounded-3xl border border-slate-200 bg-slate-50 p-8 text-center">
      <div className="mx-auto w-12 h-12 rounded-2xl bg-white border border-slate-200 flex items-center justify-center">
        <Icon size={22} className="text-slate-700" />
      </div>
      <div className="mt-3 text-lg font-black text-slate-900">{title}</div>
      <div className="mt-1 text-sm text-slate-500 font-bold">{desc}</div>
      {loading ? (
        <div className="mt-4 text-xs text-slate-400 font-extrabold">
          Loading…
        </div>
      ) : null}
    </div>
  );
}

export function MiniRow({ icon: Icon, label, value }) {
  return (
    <div className="flex items-center gap-2 text-xs font-extrabold text-slate-700">
      <Icon size={14} className="text-slate-400" />
      <span className="text-slate-500">{label}:</span>
      <span className="text-slate-900">{value || "—"}</span>
    </div>
  );
}

/* ====== Grids ====== */
export function MyGrid(props) {
  const {
    authed,
    myTrips,
    myShipments,
    savedList,
    currentUserId,
    saved,
    getRequestState,
    onOpenTrip,
    onOpenShipment,
    onRequest,
    onSave,
    onEditTrip,
    onDeleteTrip,
    onCreateTrip,
    onCreateShipment,
    onEditShipment,
    onDeleteShipment,
    onLogin,
  } = props;

  if (!authed) {
    return (
      <div className="space-y-3">
        <div className="rounded-2xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Lock size={18} />
            Login to see your trips, shipments and saved items.
          </div>
          <button
            onClick={() => onLogin?.()}
            className="px-4 py-2 rounded-xl bg-slate-900 text-white font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <LogIn size={16} />
            Login
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-8">
      {/* MY TRIPS */}
      <section className="space-y-4">
        <div className="flex items-center justify-between gap-2">
          <div className="text-lg font-black text-slate-900">My Trips</div>
          <button
            onClick={() => onCreateTrip?.()}
            className="px-4 py-2 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <Plus size={16} />
            New Trip
          </button>
        </div>

        {!myTrips?.length ? (
          <EmptyState
            icon={Plane}
            title="No trips"
            desc="Create a trip and start getting requests."
            loading={false}
          />
        ) : (
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {(myTrips || []).map((it) => {
              const owner =
                !!currentUserId &&
                !!it?._ownerId &&
                String(it._ownerId) === String(currentUserId);

              return (
                <CarryCard
                  key={`my_trip_${it.id}`}
                  kind="trip"
                  it={it}
                  saved={saved}
                  currentUserId={currentUserId}
                  loading={false}
                  requestState={getRequestState?.(it)}
                  flags={{
                    showSave: true,
                    showPrimary: true,
                    showDetails: true,
                    showOwnerActions: owner,
                    showImage: false,
                  }}
                  onOpen={() => onOpenTrip(it.id)}
                  onRequest={() => onRequest?.(it.id)}
                  onSave={() => onSave?.(it.id)}
                  onEdit={isTripLocked(it) ? undefined : () => onEditTrip?.(it)}
                  onDelete={
                    isTripLocked(it) ? undefined : () => onDeleteTrip?.(it.id)
                  }
                  onLogin={onLogin}
                />
              );
            })}
          </div>
        )}
      </section>

      {/* MY SHIPMENTS */}
      <section className="space-y-4">
        <div className="flex items-center justify-between gap-2">
          <div className="text-lg font-black text-slate-900">My Shipments</div>
          <button
            onClick={() => onCreateShipment?.()}
            className="px-4 py-2 rounded-2xl bg-white border border-slate-200 hover:bg-slate-50 text-sm font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <Plus size={16} />
            New Shipment
          </button>
        </div>

        {!myShipments?.length ? (
          <EmptyState
            icon={Package}
            title="No shipments"
            desc="Create a shipment so you can request trips."
            loading={false}
          />
        ) : (
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {(myShipments || []).map((raw) => {
              const it = normalizeShipment(raw);
              const owner =
                !!currentUserId &&
                !!it?._ownerId &&
                String(it._ownerId) === String(currentUserId);

              const locked = isShipmentLocked(it);

              return (
                <CarryCard
                  key={`my_shipment_${it.id}`}
                  kind="shipment"
                  it={it}
                  saved={new Set()}
                  currentUserId={currentUserId}
                  loading={false}
                  flags={{
                    showSave: false,
                    showPrimary: false,
                    showDetails: true,
                    showOwnerActions: owner && !locked,
                    showImage: true,
                  }}
                  onOpen={() => onOpenShipment(it)}
                  onEdit={locked ? undefined : () => onEditShipment?.(it)}
                  onDelete={
                    locked ? undefined : () => onDeleteShipment?.(it.id)
                  }
                  onLogin={onLogin}
                />
              );
            })}
          </div>
        )}
      </section>

      {/* SAVED */}
      <section className="space-y-4">
        <div className="flex items-center justify-between gap-2">
          <div className="text-lg font-black text-slate-900">Saved</div>
          <Pill tone="rose">
            <Heart size={14} /> {savedList?.length || 0}
          </Pill>
        </div>

        {!savedList?.length ? (
          <EmptyState
            icon={Heart}
            title="No saved items"
            desc="Save trips or shipments from Explore."
            loading={false}
          />
        ) : (
          <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
            {(savedList || []).map((it) => {
              const owner =
                !!currentUserId &&
                !!it?._ownerId &&
                String(it._ownerId) === String(currentUserId);

              const locked =
                it._type === "trip" ? isTripLocked(it) : isShipmentLocked(it);

              return (
                <CarryCard
                  key={`saved_${it._type}_${it.id}`}
                  kind={it._type}
                  it={it}
                  saved={saved}
                  currentUserId={currentUserId}
                  loading={false}
                  requestState={
                    it._type === "trip" ? getRequestState?.(it) : undefined
                  }
                  flags={{
                    showSave: true,
                    showPrimary: it._type === "trip",
                    showDetails: true,
                    showOwnerActions: owner && !locked,
                    showImage: it._type === "shipment",
                  }}
                  onOpen={() =>
                    it._type === "trip" ? onOpenTrip(it.id) : onOpenShipment(it)
                  }
                  onRequest={
                    it._type === "trip" ? () => onRequest?.(it.id) : undefined
                  }
                  onSave={() => onSave?.(it.id)}
                  onEdit={
                    locked
                      ? undefined
                      : it._type === "trip"
                      ? () => onEditTrip?.(it)
                      : () => onEditShipment?.(it)
                  }
                  onDelete={
                    locked
                      ? undefined
                      : it._type === "trip"
                      ? () => onDeleteTrip?.(it.id)
                      : () => onDeleteShipment?.(it.id)
                  }
                  onLogin={onLogin}
                />
              );
            })}
          </div>
        )}
      </section>
    </div>
  );
}

export function ShipmentsGrid(props) {
  const {
    authed,
    shipLoading,
    myShipments,
    currentUserId,
    onCreate,
    onEdit,
    onDelete,
    onOpenShipment,
    onLogin,
  } = props;

  const empty = !myShipments?.length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <div className="text-lg font-black text-slate-900">My Shipments</div>
        <div className="flex items-center gap-2">
          <Pill tone="slate">
            <Package size={14} /> {myShipments?.length || 0}
          </Pill>
          <button
            onClick={() => (authed ? onCreate?.() : onLogin?.())}
            className="px-4 py-2 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <Plus size={16} />
            New Shipment
          </button>
        </div>
      </div>

      {!authed ? (
        <div className="rounded-2xl border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Lock size={18} />
            Login to create and manage shipments.
          </div>
          <button
            onClick={() => onLogin?.()}
            className="px-4 py-2 rounded-xl bg-slate-900 text-white font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <LogIn size={16} />
            Login
          </button>
        </div>
      ) : null}

      {authed && empty ? (
        <EmptyState
          icon={Package}
          title="No shipments yet"
          desc="Create a shipment so you can request trips."
          loading={shipLoading}
        />
      ) : authed ? (
        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {(myShipments || []).map((raw) => {
            const it = normalizeShipment(raw);
            const owner =
              !!currentUserId &&
              !!it?._ownerId &&
              String(it._ownerId) === String(currentUserId);

            const locked = isShipmentLocked(it);

            return (
              <CarryCard
                key={`shipment_${it.id}`}
                kind="shipment"
                it={it}
                saved={new Set()}
                currentUserId={currentUserId}
                loading={shipLoading}
                flags={{
                  showSave: false,
                  showPrimary: false,
                  showDetails: true,
                  showOwnerActions: owner && !locked,
                  showImage: true,
                }}
                onOpen={() => onOpenShipment?.(it.id)}
                onEdit={locked ? undefined : () => onEdit?.(it)}
                onDelete={locked ? undefined : () => onDelete?.(it.id)}
                onLogin={onLogin}
              />
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

export function TripsGrid(props) {
  const {
    loading,
    trips,
    saved,
    currentUserId,
    getRequestState,
    onOpenTrip,
    onRequest,
    onSave,
    onEdit,
    onDelete,
    onCreate,
    onLogin,
    authed,
  } = props;

  const empty = !trips?.length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <div className="text-lg font-black text-slate-900">Trips</div>
        <div className="flex items-center gap-2">
          <Pill tone="slate">
            <Plane size={14} /> {trips?.length || 0}
          </Pill>
          <button
            onClick={() => (authed ? onCreate?.() : onLogin?.())}
            className="px-4 py-2 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 text-sm font-extrabold inline-flex items-center gap-2"
            type="button"
          >
            <Plus size={16} />
            New Trip
          </button>
        </div>
      </div>

      {empty ? (
        <EmptyState
          icon={Plane}
          title="No trips"
          desc="Create a trip so senders can request you."
          loading={loading}
        />
      ) : (
        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {(trips || []).map((it) => {
            const owner =
              !!currentUserId &&
              !!it?._ownerId &&
              String(it._ownerId) === String(currentUserId);

            return (
              <CarryCard
                key={`trip_${it.id}`}
                kind="trip"
                it={it}
                saved={saved}
                currentUserId={currentUserId}
                loading={loading}
                requestState={getRequestState?.(it)}
                flags={{
                  showSave: true,
                  showPrimary: true,
                  showDetails: true,
                  showOwnerActions: owner,
                  showImage: false,
                }}
                onOpen={() => onOpenTrip(it.id)}
                onRequest={() => onRequest(it.id)}
                onSave={() => onSave(it.id)}
                onEdit={isTripLocked(it) ? undefined : () => onEdit?.(it)}
                onDelete={
                  isTripLocked(it) ? undefined : () => onDelete?.(it.id)
                }
                onLogin={onLogin}
              />
            );
          })}
        </div>
      )}
    </div>
  );
}

export function ExploreGrid(props) {
  const {
    loading,
    items,
    saved,
    currentUserId,
    getRequestState,
    onOpenTrip,
    onOpenShipment,
    onRequest,
    onSave,
    onEditShipment,
    onDeleteShipment,
    onEditTrip,
    onDeleteTrip,
    onLogin,
  } = props;

  const empty = !items?.length;

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-2">
        <div className="text-lg font-black text-slate-900">Explore</div>
        <Pill tone="indigo">
          <Search size={14} /> {items?.length || 0} items
        </Pill>
      </div>

      {empty ? (
        <EmptyState
          icon={Search}
          title="No results"
          desc="Try changing filters or search."
          loading={loading}
        />
      ) : (
        <div className="grid sm:grid-cols-2 lg:grid-cols-3 gap-3">
          {items.map((raw) => {
            const it =
              raw?._type === "shipment"
                ? normalizeShipment(raw)
                : raw?._type === "trip"
                ? normalizeTrip(raw)
                : raw;

            const owner =
              !!currentUserId &&
              !!it?._ownerId &&
              String(it._ownerId) === String(currentUserId);

            const locked =
              it?._type === "trip" ? isTripLocked(it) : isShipmentLocked(it);

            return (
              <CarryCard
                key={`${it._type}_${it.id}`}
                kind={it._type}
                it={it}
                saved={saved}
                currentUserId={currentUserId}
                loading={loading}
                requestState={
                  it._type === "trip" ? getRequestState(it) : undefined
                }
                flags={{
                  showSave: true,
                  showPrimary: it._type === "trip",
                  showDetails: true,
                  showOwnerActions: owner && !locked,
                  showImage: it._type === "shipment",
                }}
                onOpen={() =>
                  it._type === "trip" ? onOpenTrip(it.id) : onOpenShipment(it)
                }
                onRequest={
                  it._type === "trip" ? () => onRequest(it.id) : undefined
                }
                onSave={() => onSave(it.id)}
                onEdit={
                  locked
                    ? undefined
                    : it._type === "trip"
                    ? () => onEditTrip?.(it)
                    : () => onEditShipment?.(it)
                }
                onDelete={
                  locked
                    ? undefined
                    : it._type === "trip"
                    ? () => onDeleteTrip?.(it.id)
                    : () => onDeleteShipment?.(it.id)
                }
                onLogin={onLogin}
              />
            );
          })}
        </div>
      )}
    </div>
  );
}
