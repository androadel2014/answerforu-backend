import React from "react";
import { X } from "lucide-react";

export function FiltersDrawer({
  open,
  onClose,
  fromCountry,
  setFromCountry,
  toCountry,
  setToCountry,
  minWeight,
  setMinWeight,
  maxReward,
  setMaxReward,
  loading,
  onReset,
  onApply,
}) {
  if (!open) return null;

  return (
    <div className="fixed inset-0 z-[9999] bg-black/40 backdrop-blur-sm flex items-end sm:items-center justify-center p-3">
      <div className="w-full max-w-xl rounded-3xl bg-white border border-slate-200 shadow-2xl overflow-hidden">
        <div className="p-4 border-b border-slate-100 flex items-center justify-between">
          <div>
            <div className="text-lg font-black text-slate-900">Filters</div>
            <div className="text-xs text-slate-500">
              Fast filters • clean layout
            </div>
          </div>
          <button
            onClick={onClose}
            className="w-10 h-10 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 flex items-center justify-center"
          >
            <X size={18} />
          </button>
        </div>

        <div className="p-4 grid sm:grid-cols-2 gap-3">
          <input
            value={fromCountry}
            onChange={(e) => setFromCountry(e.target.value)}
            placeholder="From country (exact)"
            className="px-3 py-2.5 rounded-2xl border border-slate-200"
          />
          <input
            value={toCountry}
            onChange={(e) => setToCountry(e.target.value)}
            placeholder="To country (exact)"
            className="px-3 py-2.5 rounded-2xl border border-slate-200"
          />

          <input
            value={minWeight}
            onChange={(e) => setMinWeight(e.target.value)}
            placeholder="Min weight (kg)"
            className="px-3 py-2.5 rounded-2xl border border-slate-200"
          />
          <input
            value={maxReward}
            onChange={(e) => setMaxReward(e.target.value)}
            placeholder="Max reward"
            className="px-3 py-2.5 rounded-2xl border border-slate-200"
          />
        </div>

        <div className="p-4 border-t border-slate-100 flex gap-2 justify-end">
          <button
            onClick={onReset}
            className="px-4 py-2.5 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 font-extrabold text-sm"
          >
            Reset
          </button>
          <button
            disabled={loading}
            onClick={onApply}
            className="px-5 py-2.5 rounded-2xl bg-slate-900 text-white hover:bg-slate-800 font-extrabold text-sm disabled:opacity-60"
          >
            Apply
          </button>
        </div>
      </div>
    </div>
  );
}

export function MobileDetailsDrawer({ open, onClose, details, children }) {
  if (!open || !details?.item) return null;

  return (
    <div className="fixed inset-0 z-[9999] bg-black/40 backdrop-blur-sm flex items-end lg:hidden">
      <div className="w-full rounded-t-3xl bg-white border-t border-slate-200 shadow-2xl max-h-[92vh] overflow-auto">
        <div className="p-3 border-b border-slate-100 flex items-center justify-between">
          <div className="text-sm font-black text-slate-900">
            Listing #{details.item.id}
          </div>
          <button
            onClick={onClose}
            className="w-10 h-10 rounded-2xl border border-slate-200 bg-white hover:bg-slate-50 flex items-center justify-center"
          >
            <X size={18} />
          </button>
        </div>

        <div className="p-4">{children}</div>
      </div>
    </div>
  );
}
