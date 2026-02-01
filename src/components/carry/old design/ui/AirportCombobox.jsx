import React from "react";
import { airportLabel, cn } from "../shared/carryUtils";
import { searchAirports } from "../shared/carryApi";

export default function AirportCombobox({
  label,
  selected,
  onChange,
  placeholder = "Search airport…",
}) {
  const [open, setOpen] = React.useState(false);
  const [q, setQ] = React.useState("");
  const ref = React.useRef(null);

  React.useEffect(() => {
    function onDoc(e) {
      if (!ref.current) return;
      if (!ref.current.contains(e.target)) setOpen(false);
    }
    document.addEventListener("mousedown", onDoc);
    return () => document.removeEventListener("mousedown", onDoc);
  }, []);

  const [options, setOptions] = React.useState([]);
  const [loadingAir, setLoadingAir] = React.useState(false);

  React.useEffect(() => {
    const text = q.trim();
    if (text.length < 2) {
      setOptions([]);
      return;
    }

    const timer = setTimeout(async () => {
      try {
        setLoadingAir(true);
        const r = await searchAirports(text);

        const arr = Array.isArray(r)
          ? r
          : Array.isArray(r?.items)
          ? r.items
          : Array.isArray(r?.data)
          ? r.data
          : Array.isArray(r?.rows)
          ? r.rows
          : Array.isArray(r?.result)
          ? r.result
          : [];

        setOptions(
          arr.map((x) => ({
            code: x.iata || x.icao || "",
            city: x.city || "",
            country: x.country || x.country_code || "",
            name: x.name || "",
          }))
        );
      } catch {
        setOptions([]);
      } finally {
        setLoadingAir(false);
      }
    }, 250);

    return () => clearTimeout(timer);
  }, [q]);

  const list = options;

  return (
    <div ref={ref} className="relative">
      <div className="text-[11px] font-extrabold tracking-wide text-slate-600 uppercase mb-1">
        {label}
      </div>

      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className={cn(
          "w-full px-3 py-3 rounded-2xl border bg-white text-left text-sm font-bold",
          "border-slate-200 focus:outline-none focus:ring-2 focus:ring-indigo-200"
        )}
      >
        {selected?.code ? (
          airportLabel(selected)
        ) : (
          <span className="text-slate-400">{placeholder}</span>
        )}
      </button>

      {open ? (
        <div className="absolute z-50 mt-2 w-full rounded-2xl border border-slate-200 bg-white shadow-xl overflow-hidden">
          <div className="p-2 border-b border-slate-100">
            <input
              autoFocus
              value={q}
              onChange={(e) => setQ(e.target.value)}
              placeholder="Type: CAI, Cairo, Dulles…"
              className="w-full px-3 py-2.5 rounded-2xl border border-slate-200 bg-slate-50 focus:outline-none focus:ring-2 focus:ring-indigo-200 text-sm font-semibold"
            />
          </div>

          <div className="max-h-72 overflow-auto">
            {loadingAir ? (
              <div className="p-3 text-sm text-slate-500">Searching…</div>
            ) : list.length ? (
              list.map((a) => (
                <button
                  key={a.code || `${a.city}-${a.name}`}
                  type="button"
                  onClick={() => {
                    onChange(a);
                    setOpen(false);
                    setQ("");
                  }}
                  className="w-full text-left px-3 py-3 hover:bg-slate-50 border-b border-slate-100"
                >
                  <div className="text-sm font-extrabold text-slate-900">
                    {a.city || "—"}{" "}
                    <span className="text-slate-400">({a.code || "—"})</span>
                  </div>
                  <div className="text-xs text-slate-500 font-semibold">
                    {a.country || "—"} • {a.name || "—"}
                  </div>
                </button>
              ))
            ) : (
              <div className="p-3 text-sm text-slate-500">No matches.</div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  );
}
