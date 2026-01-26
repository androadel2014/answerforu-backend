// src/components/carry/sections/CreateForm.jsx
import React from "react";
import { Plus } from "lucide-react";
import Field from "../ui/Field";
import AirportCombobox from "../ui/AirportCombobox";
import { emptyForm } from "../shared/carryUtils";

export default function CreateForm({
  loading,
  editId,
  form,
  setForm,
  onSubmit,
  onCancelEdit,
}) {
  // ✅ TRIP ONLY (Traveler)
  const tripForm = {
    ...emptyForm,
    ...form,
    role: "traveler",
  };

  // keep parent state in sync (no role switching anymore)
  function setTrip(patch) {
    setForm((s) => ({ ...s, ...patch, role: "traveler" }));
  }

  return (
    <div className="p-4 sm:p-6">
      <div className="max-w-5xl">
        <div className="rounded-3xl border border-slate-200 bg-white shadow-sm overflow-hidden">
          <div className="p-6 bg-gradient-to-r from-slate-950 via-slate-900 to-indigo-950 text-white">
            <div className="flex items-center justify-between gap-3">
              <div>
                <div className="text-2xl font-black tracking-tight">
                  {editId ? `Edit Trip #${editId}` : "Create Trip"}
                </div>
                <div className="text-sm text-white/75 mt-1">
                  Post your travel trip (From → To). Senders will attach their
                  shipments to your trip.
                </div>
              </div>

              <div className="hidden sm:flex items-center gap-2">
                {editId ? (
                  <button
                    disabled={loading}
                    onClick={onCancelEdit}
                    className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold disabled:opacity-60"
                  >
                    Cancel
                  </button>
                ) : null}

                <button
                  disabled={loading}
                  onClick={() => setForm({ ...emptyForm, role: "traveler" })}
                  className="px-4 py-2 rounded-2xl bg-white/10 hover:bg-white/15 border border-white/15 text-sm font-extrabold disabled:opacity-60"
                >
                  Reset
                </button>

                <button
                  disabled={loading}
                  onClick={() => {
                    // ✅ force role traveler before submit
                    setForm((s) => ({ ...s, role: "traveler" }));
                    onSubmit?.();
                  }}
                  className="px-4 py-2 rounded-2xl bg-white text-slate-900 hover:bg-slate-100 text-sm font-extrabold disabled:opacity-60 flex items-center gap-2"
                >
                  <Plus size={16} />
                  {editId ? "Update" : "Create"}
                </button>
              </div>
            </div>
          </div>

          <div className="p-6">
            <div className="grid lg:grid-cols-2 gap-6">
              {/* LEFT */}
              <div className="space-y-4">
                <div className="text-sm font-black text-slate-900">
                  Trip route
                </div>

                <div className="text-sm font-black text-slate-900 mt-2">
                  Airports
                </div>

                <AirportCombobox
                  label="From airport"
                  selected={
                    tripForm.from_airport
                      ? {
                          code: tripForm.from_airport,
                          city: tripForm.from_city,
                          country: tripForm.from_country,
                          name: tripForm.from_airport_name || "",
                        }
                      : null
                  }
                  onChange={(a) => {
                    setTrip({
                      from_airport: a.code,
                      from_city: a.city,
                      from_country: a.country,
                      from_airport_name: a.name,
                    });
                  }}
                />

                <AirportCombobox
                  label="To airport"
                  selected={
                    tripForm.to_airport
                      ? {
                          code: tripForm.to_airport,
                          city: tripForm.to_city,
                          country: tripForm.to_country,
                          name: tripForm.to_airport_name || "",
                        }
                      : null
                  }
                  onChange={(a) => {
                    setTrip({
                      to_airport: a.code,
                      to_city: a.city,
                      to_country: a.country,
                      to_airport_name: a.name,
                    });
                  }}
                />

                <div className="grid sm:grid-cols-2 gap-3">
                  <Field label="Travel date & time">
                    <input
                      type="datetime-local"
                      value={tripForm.travel_date}
                      onChange={(e) => setTrip({ travel_date: e.target.value })}
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                    />
                  </Field>

                  <Field label="Arrival date & time (optional)">
                    <input
                      type="datetime-local"
                      value={tripForm.arrival_date}
                      onChange={(e) =>
                        setTrip({ arrival_date: e.target.value })
                      }
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                    />
                  </Field>
                </div>

                <div className="rounded-3xl border border-slate-200 bg-slate-50 p-4 space-y-3">
                  <div className="text-sm font-black text-slate-900">
                    Traveler details
                  </div>

                  <div className="grid sm:grid-cols-2 gap-3">
                    <Field label="Airline (optional)">
                      <input
                        value={tripForm.airline}
                        onChange={(e) => setTrip({ airline: e.target.value })}
                        placeholder="e.g. Emirates"
                        className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                      />
                    </Field>

                    <Field label="Flight number (optional)">
                      <input
                        value={tripForm.flight_number}
                        onChange={(e) =>
                          setTrip({ flight_number: e.target.value })
                        }
                        placeholder="e.g. EK 231"
                        className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white"
                      />
                    </Field>
                  </div>

                  <Field label="Max item size">
                    <select
                      value={tripForm.max_item_size}
                      onChange={(e) =>
                        setTrip({ max_item_size: e.target.value })
                      }
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                    >
                      <option value="small">Small (documents)</option>
                      <option value="medium">Medium (shoes/phone)</option>
                      <option value="large">Large (laptop/bag)</option>
                    </select>
                  </Field>

                  <Field label="Meet preference">
                    <select
                      value={tripForm.meet_pref}
                      onChange={(e) => setTrip({ meet_pref: e.target.value })}
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white text-sm font-bold"
                    >
                      <option value="airport">At airport</option>
                      <option value="nearby">Nearby</option>
                      <option value="city">In city</option>
                    </select>
                  </Field>
                </div>
              </div>

              {/* RIGHT */}
              <div className="space-y-4">
                <div className="text-sm font-black text-slate-900">
                  Capacity & reward
                </div>

                <div className="grid sm:grid-cols-2 gap-3">
                  <Field label="Available weight (kg)">
                    <input
                      type="number"
                      value={tripForm.available_weight}
                      onChange={(e) =>
                        setTrip({ available_weight: e.target.value })
                      }
                      placeholder="e.g. 10"
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                    />
                  </Field>

                  <Field label="Reward amount (USD)">
                    <input
                      type="number"
                      value={tripForm.reward_amount}
                      onChange={(e) =>
                        setTrip({ reward_amount: e.target.value })
                      }
                      placeholder="e.g. 50"
                      className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                    />
                  </Field>
                </div>

                <Field label="Item type (what you can carry)">
                  <input
                    value={tripForm.item_type}
                    onChange={(e) => setTrip({ item_type: e.target.value })}
                    placeholder="documents, electronics…"
                    className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  />
                </Field>

                <Field label="Description">
                  <textarea
                    value={tripForm.description}
                    onChange={(e) => setTrip({ description: e.target.value })}
                    placeholder="Rules, restrictions, meet notes…"
                    className="w-full px-3 py-3 rounded-2xl border border-slate-200 bg-white min-h-[210px] focus:outline-none focus:ring-2 focus:ring-indigo-200"
                  />
                </Field>

                <div className="sm:hidden flex gap-2 pt-2">
                  <button
                    disabled={loading}
                    onClick={() => setForm({ ...emptyForm, role: "traveler" })}
                    className="flex-1 px-4 py-3 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold disabled:opacity-60"
                  >
                    Reset
                  </button>
                  <button
                    disabled={loading}
                    onClick={() => {
                      setForm((s) => ({ ...s, role: "traveler" }));
                      onSubmit?.();
                    }}
                    className="flex-1 px-4 py-3 rounded-2xl bg-indigo-600 text-white hover:bg-indigo-700 font-extrabold disabled:opacity-60"
                  >
                    {editId ? "Update" : "Create"}
                  </button>
                </div>

                <div className="text-xs text-slate-500 font-bold">
                  Currency is fixed to{" "}
                  <span className="text-slate-900">USD</span>.
                </div>
              </div>
            </div>

            {/* hidden role hard-set */}
            <input type="hidden" value="traveler" readOnly />
          </div>
        </div>
      </div>
    </div>
  );
}
