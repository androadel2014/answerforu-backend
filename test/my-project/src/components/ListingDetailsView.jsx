// src/components/ListingDetailsView.jsx
import React from "react";
import { Navigate, useParams } from "react-router-dom";

// ✅ keep the named export working (backward-compatible)
export { CardItem } from "./community/CardItem";

export default function ListingDetailsView() {
  const { type, id, placeId } = useParams();
  const theId = id || placeId;
  const t = String(type || "places").toLowerCase();

  // ✅ send old details page to the single canonical page (ItemDetailsView )
  return <Navigate to={`/marketplace/${t}/${theId}`} replace />;
}
