import React from "react";

export default function Field({ label, hint, children }) {
  return (
    <label className="block">
      <div className="flex items-center justify-between gap-2 mb-1">
        <div className="text-[11px] font-extrabold tracking-wide text-slate-600 uppercase">
          {label}
        </div>
        {hint ? (
          <div className="text-[11px] text-slate-400 font-semibold">{hint}</div>
        ) : null}
      </div>
      {children}
    </label>
  );
}
