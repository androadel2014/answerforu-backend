// src/components/ProfilePage.jsx  (FULL FILE - copy/paste)

import PostCard from "./profile/PostCard";
import React, { useCallback, useEffect, useMemo, useState } from "react";
import { useParams, useNavigate, useLocation } from "react-router-dom";
import toast from "react-hot-toast";
import PostComposer from "../components/feed/PostComposer";
import { MessageCircle } from "lucide-react";

import ProfilePageBody from "./profile/ProfilePageBody";

import {
  tt,
  getDir,
  getAPIBase,
  authHeaders,
  isAuthed,
  extractNumericId,
  getAuthUserId,
  getPostId,
  normId,
  toastConfirm,
  tryFetchFallback,
  absUrl,
  toArr,
  uniq,
  buildUpdateFormData,
  normalizePostForMedia,
} from "./profile/profilePage.parts";

/* =========================
   Marketplace Types
========================= */
const MARKET_TYPES = ["services", "products", "jobs", "housing"];

/* =========================
   ✅ Profile normalize + single source of truth
   - Display reads from normalized "profile"
   - Edit form is derived from the SAME normalized "profile"
   - Save updates the SAME normalized "profile"
========================= */
function normalizeProfile(p) {
  const x = p || {};

  const username = String(
    x.username ??
      x.user_name ??
      x.userName ??
      x.handle ??
      x.display_name ??
      x.displayName ??
      x.full_name ??
      x.fullName ??
      "",
  ).trim();

  const display_name = String(
    x.display_name ??
      x.displayName ??
      x.full_name ??
      x.fullName ??
      x.name ??
      x.username ??
      username ??
      "",
  ).trim();

  const phone = String(
    x.phone ??
      x.phone_number ??
      x.phoneNumber ??
      x.mobile ??
      x.mobile_number ??
      x.tel ??
      "",
  ).trim();

  const whatsapp = String(
    x.whatsapp ?? x.whatsApp ?? x.wa ?? x.whatsapp_number ?? "",
  ).trim();

  const location = String(
    x.location ??
      x.address ??
      x.address_line ??
      x.addressLine ??
      x.city_state ??
      "",
  ).trim();

  const bio = String(x.bio ?? x.about ?? x.summary ?? "").trim();

  const website = String(x.website ?? x.site ?? x.url ?? "").trim();

  const avatar_url = String(
    x.avatar_url ?? x.avatar ?? x.avatarUrl ?? "",
  ).trim();

  const cover_url = String(x.cover_url ?? x.cover ?? x.coverUrl ?? "").trim();

  return {
    ...(x || {}),
    username,
    display_name,
    phone,
    whatsapp,
    location,
    bio,
    website,
    avatar_url,
    cover_url,
  };
}

function toEditFormFromProfile(p) {
  const x = normalizeProfile(p || {});
  return {
    username: x.username || "",
    display_name: x.display_name || "",
    avatar_url: x.avatar_url || "",
    cover_url: x.cover_url || "",
    bio: x.bio || "",
    location: x.location || "",
    phone: x.phone || "",
    whatsapp: x.whatsapp || "",
    website: x.website || "",
  };
}

function buildProfileSavePayload(editForm) {
  const f = editForm || {};
  return {
    // ✅ username
    username: f.username,
    user_name: f.username,
    handle: f.username,

    // ✅ display name
    display_name: f.display_name,
    displayName: f.display_name,
    full_name: f.display_name,
    fullName: f.display_name,

    // ✅ phone
    phone: f.phone,
    phone_number: f.phone,
    phoneNumber: f.phone,
    mobile: f.phone,

    // ✅ whatsapp
    whatsapp: f.whatsapp,
    whatsApp: f.whatsapp,
    wa: f.whatsapp,

    // ✅ location/address
    location: f.location,
    address: f.location,
    address_line: f.location,
    addressLine: f.location,

    // ✅ bio
    bio: f.bio,
    about: f.bio,
    summary: f.bio,

    // ✅ website
    website: f.website,
    site: f.website,
    url: f.website,

    // ✅ avatar/cover
    avatar_url: f.avatar_url,
    avatarUrl: f.avatar_url,
    avatar: f.avatar_url,

    cover_url: f.cover_url,
    coverUrl: f.cover_url,
    cover: f.cover_url,
  };
}

function mergeProfileWithEditForm(prevProfile, editForm) {
  const base = normalizeProfile(prevProfile || {});
  const f = editForm || {};
  const merged = {
    ...base,
    username: String(f.username ?? base.username ?? "").trim(),
    display_name: String(f.display_name ?? base.display_name ?? "").trim(),
    avatar_url: String(f.avatar_url ?? base.avatar_url ?? "").trim(),
    cover_url: String(f.cover_url ?? base.cover_url ?? "").trim(),
    bio: String(f.bio ?? base.bio ?? "").trim(),
    location: String(f.location ?? base.location ?? "").trim(),
    phone: String(f.phone ?? base.phone ?? "").trim(),
    whatsapp: String(f.whatsapp ?? base.whatsapp ?? "").trim(),
    website: String(f.website ?? base.website ?? "").trim(),
  };
  return normalizeProfile(merged);
}

/* =========================
   Helpers
========================= */
function buildPrefixedId(type, id) {
  const t = String(type || "").toLowerCase();
  const n = id == null ? null : Number(id);
  if (!Number.isFinite(n)) return null;

  if (t === "places") return `place_${n}`;
  if (t === "groups") return `group_${n}`;
  if (t === "services") return `service_${n}`;
  if (t === "products") return `product_${n}`;
  if (t === "jobs") return `jobs_${n}`;
  if (t === "housing") return `housing_${n}`;
  return null;
}

function extractArray(r) {
  if (Array.isArray(r)) return r;
  if (Array.isArray(r?.items)) return r.items;
  if (Array.isArray(r?.data)) return r.data;
  if (Array.isArray(r?.results)) return r.results;
  if (Array.isArray(r?.rows)) return r.rows;
  if (Array.isArray(r?.posts)) return r.posts;
  return [];
}

// ✅ reviews extract + merge helpers
function extractReviewsArray(r) {
  if (Array.isArray(r)) return r;
  if (Array.isArray(r?.reviews)) return r.reviews;
  if (Array.isArray(r?.items)) return r.items;
  if (Array.isArray(r?.data)) return r.data;
  if (Array.isArray(r?.results)) return r.results;
  if (Array.isArray(r?.rows)) return r.rows;
  if (Array.isArray(r?.latest)) return r.latest;
  return [];
}

function reviewKey(x) {
  const id = x?.id ?? x?.review_id ?? x?.rating_id ?? "";
  if (id) return `id:${id}`;
  const rid = x?.reviewer_id ?? x?.reviewerId ?? x?.user_id ?? x?.userId ?? "";
  const itemId = x?.item_id ?? x?.itemId ?? "";
  const t =
    x?.item_type ??
    x?.itemType ??
    x?.listing_type ??
    x?.listingType ??
    x?.type ??
    "";
  const c = String(
    x?.comment ?? x?.commentText ?? x?.text ?? x?.body ?? x?.content ?? "",
  ).trim();
  const at = x?.created_at ?? x?.createdAt ?? x?.time ?? "";
  return `k:${rid}|${t}|${itemId}|${at}|${c}`;
}

function mergeUniqueReviews(a, b) {
  const out = [];
  const seen = new Set();
  const A = Array.isArray(a) ? a : [];
  const B = Array.isArray(b) ? b : [];
  for (const x of [...A, ...B]) {
    const k = reviewKey(x);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(x);
  }
  return out;
}

function getOwnerId(x) {
  return (
    x?.created_by ??
    x?.createdBy ??
    x?.user_id ??
    x?.userId ??
    x?.owner_id ??
    x?.ownerId ??
    x?.created_by_id ??
    x?.createdById ??
    x?.createdByUserId ??
    0
  );
}

function getRawId(x) {
  return (
    x?.id ??
    x?.listing_id ??
    x?.service_id ??
    x?.product_id ??
    x?.job_id ??
    x?.housing_id ??
    x?.place_id ??
    x?.group_id ??
    x?.item_id ??
    null
  );
}

// ✅ normalize review row for UI (name + stars + clickable hrefs)
function toStars(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) return 0;
  const x = Math.round(n);
  return Math.max(0, Math.min(5, x));
}

function pickFirstStr(...xs) {
  for (const v of xs) {
    const s = String(v ?? "").trim();
    if (s) return s;
  }
  return "";
}

function normalizeReviewForUI(r) {
  const stars = toStars(
    r?.stars ??
      r?.rating ??
      r?.value ??
      r?.starsValue ??
      r?.ratingValue ??
      r?.stars?.value ??
      r?.rating?.value,
  );

  const comment = pickFirstStr(
    r?.comment,
    r?.commentText,
    r?.reviewText,
    r?.text,
    r?.body,
    r?.content,
    r?.message,
    r?.review,
  );

  const reviewerId = Number(
    r?.reviewer_id ?? r?.reviewerId ?? r?.user_id ?? r?.userId ?? 0,
  );

  const reviewerName =
    pickFirstStr(
      r?.reviewer_name,
      r?.reviewerName,
      r?.user_name,
      r?.userName,
      r?.username,
      r?.email,
    ) || "User";

  const itemTitle = pickFirstStr(
    r?.item_title,
    r?.itemTitle,
    r?.title,
    r?.listing_title,
    r?.listingTitle,
  );

  const itemType = String(
    r?.item_type ??
      r?.itemType ??
      r?.listing_type ??
      r?.listingType ??
      r?.type ??
      "",
  )
    .trim()
    .toLowerCase();

  const itemIdNum = Number(r?.item_id ?? r?.itemId ?? r?.place_id ?? 0);

  const itemPrefixed =
    itemType && itemIdNum ? buildPrefixedId(itemType, itemIdNum) : null;

  // ✅ FIX: your frontend route is /marketplace/item/:prefixedId (place_28)
  const itemHref = itemPrefixed
    ? `/marketplace/item/${itemPrefixed}`
    : itemIdNum
      ? `/marketplace/item/${itemIdNum}`
      : null;

  const reviewerHref = reviewerId ? `/u/${reviewerId}` : null;

  return {
    ...(r || {}),
    stars,
    comment,
    user_name: reviewerName,
    reviewer_id: reviewerId || null,
    reviewer_name: reviewerName,
    reviewer_href: reviewerHref,
    item_id: itemIdNum || null,
    item_title: itemTitle,
    item_type: itemType || null,
    item_prefixed_id: itemPrefixed,
    item_href: itemHref,
  };
}

// ✅ fetch MY listings from same sources as Community
async function fetchUserListingsAll(API_BASE, uid) {
  const headers = { ...authHeaders() };
  const me = String(uid || "");
  const keys = new Set();

  const pushDedup = (out, type, arr) => {
    const a = Array.isArray(arr) ? arr : [];
    for (const x of a) {
      const owner = String(getOwnerId(x) || "");
      if (owner !== me) continue;

      const rawId = getRawId(x);
      const n = Number(
        String(rawId || "").includes("_") ? String(rawId).split("_")[1] : rawId,
      );
      if (!Number.isFinite(n)) continue;

      const k = `${type}:${n}`;
      if (keys.has(k)) continue;
      keys.add(k);

      out.push({ ...(x || {}), _type: type });
    }
  };

  const out = [];

  // ✅ 1) legacy places
  try {
    const r = await tryFetchFallback(
      [
        `${API_BASE}/api/community/places`,
        `${API_BASE}/api/places`,
        `${API_BASE}/api/community_places`,
      ],
      { headers },
    );
    pushDedup(out, "places", extractArray(r));
  } catch {}

  // ✅ 2) legacy groups
  try {
    const r = await tryFetchFallback(
      [
        `${API_BASE}/api/community/groups`,
        `${API_BASE}/api/groups`,
        `${API_BASE}/api/community_groups`,
      ],
      { headers },
    );
    pushDedup(out, "groups", extractArray(r));
  } catch {}

  // ✅ 3) marketplace unified types
  for (const type of MARKET_TYPES) {
    try {
      const r = await tryFetchFallback(
        [
          `${API_BASE}/api/listings?type=${encodeURIComponent(type)}`,
          `${API_BASE}/api/marketplace/listings?type=${encodeURIComponent(
            type,
          )}`,
          `${API_BASE}/api/listings/${encodeURIComponent(type)}`,
          `${API_BASE}/api/marketplace/${encodeURIComponent(type)}`,
        ],
        { headers },
      );
      pushDedup(out, type, extractArray(r));
    } catch {}
  }

  // ✅ sort newest first (best effort)
  out.sort((a, b) =>
    String(
      b.updatedAt || b.updated_at || b.createdAt || b.created_at || "",
    ).localeCompare(
      String(a.updatedAt || a.updated_at || a.createdAt || a.created_at || ""),
    ),
  );

  return out;
}

async function fetchReceivedReviewsViaListings(API_BASE, uid, listingsAll) {
  const headers = { ...authHeaders() };
  const out = [];

  const tryGet = async (urls) => {
    try {
      const r = await tryFetchFallback(urls, { headers });
      return extractReviewsArray(r);
    } catch {
      return [];
    }
  };

  for (const item of Array.isArray(listingsAll) ? listingsAll : []) {
    const type = String(item?._type || item?.type || "").toLowerCase();
    const rawId = getRawId(item);
    const idNum = Number(
      String(rawId || "").includes("_") ? String(rawId).split("_")[1] : rawId,
    );
    if (!idNum) continue;

    const title = pickFirstStr(item?.title, item?.name, item?.listing_title);

    let rows = [];

    if (type === "places") {
      rows = await tryGet([
        `${API_BASE}/api/community/places/${idNum}/reviews`,
        `${API_BASE}/api/places/${idNum}/reviews`,
        `${API_BASE}/api/community/places/${idNum}/ratings`,
        `${API_BASE}/api/places/${idNum}/ratings`,
      ]);
    } else if (type === "groups") {
      rows = await tryGet([
        `${API_BASE}/api/community/groups/${idNum}/reviews`,
        `${API_BASE}/api/groups/${idNum}/reviews`,
        `${API_BASE}/api/community/groups/${idNum}/ratings`,
        `${API_BASE}/api/groups/${idNum}/ratings`,
      ]);
    } else {
      // unified listings/services/products/jobs/housing
      const pref = buildPrefixedId(type, idNum);
      if (!pref) continue;

      rows = await tryGet([
        `${API_BASE}/api/listings/${pref}/reviews`,
        `${API_BASE}/api/listings/${encodeURIComponent(pref)}/reviews`,
        `${API_BASE}/api/marketplace/item/${pref}/reviews`,
        `${API_BASE}/api/marketplace/item/${encodeURIComponent(pref)}/reviews`,
        `${API_BASE}/api/listings/${pref}/ratings`,
        `${API_BASE}/api/marketplace/item/${pref}/ratings`,
      ]);
    }

    for (const r of Array.isArray(rows) ? rows : []) {
      out.push({
        ...(r || {}),
        item_type: type,
        item_id: idNum,
        item_title: title,
      });
    }
  }

  return out;
}

/* =========================
   Main Page
========================= */
export function ProfilePage({ lang = "en", forcedUserId = null }) {
  const API_BASE = useMemo(() => getAPIBase(), []);
  const { userId } = useParams();
  const userIdParam = String(forcedUserId ?? userId ?? "").trim();

  const navigate = useNavigate();
  const location = useLocation();
  // ✅ allows /u/me without changing App.jsx routes

  const effectiveUserId =
    userIdParam.toLowerCase() === "me"
      ? String(getAuthUserId() || "").trim()
      : userIdParam;

  const dir = getDir(lang);

  const [loading, setLoading] = useState(true);

  const [profile, setProfile] = useState(null);
  const [stats, setStats] = useState(null);
  const [ratingsStats, setRatingsStats] = useState(null);

  const [isMe, setIsMe] = useState(false);
  const [isFollowing, setIsFollowing] = useState(false);

  const [tab, setTab] = useState("posts"); // posts | listingsAll | reviews

  const [posts, setPosts] = useState([]);
  const [reviews, setReviews] = useState([]);

  // ✅ unified listings (profile owner listings)
  const [listingsAll, setListingsAll] = useState([]);
  const [listingsLoading, setListingsLoading] = useState(false);

  const [tabLoading, setTabLoading] = useState(false);
  const [cvCount, setCvCount] = useState(0);

  const [editOpen, setEditOpen] = useState(false);
  const [addReviewOpen, setAddReviewOpen] = useState(false);

  const [editForm, setEditForm] = useState({
    username: "",
    display_name: "",
    avatar_url: "",
    cover_url: "",
    bio: "",
    location: "",
    phone: "",
    whatsapp: "",
    website: "",
  });

  const [reviewForm, setReviewForm] = useState({ rating: 5, comment: "" });

  const canAct = isAuthed();
  const authedId = getAuthUserId();

  const computedIsMe =
    !!authedId &&
    !!effectiveUserId &&
    normId(authedId) === normId(String(effectiveUserId));

  const canEdit = canAct && (isMe || computedIsMe);

  const countPosts = Number(stats?.posts ?? posts.length ?? 0) || 0;
  const countReviews = Number(reviews?.length ?? 0) || 0;

  // ✅ listings belong to the PROFILE owner (the userId in URL)
  const listingsOwnerId = String(effectiveUserId || "").trim();

  const countListingsAll = listingsAll.length || 0;

  useEffect(() => {
    let dead = false;

    async function load() {
      setLoading(true);
      try {
        const uid = String(effectiveUserId || "").trim();
        if (!uid) throw new Error(tt(lang, "loginFirst") || "Login first");

        const data = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}`,
            `${API_BASE}/api/profiles/${uid}`,
            `${API_BASE}/api/user/${uid}/profile`,
            `${API_BASE}/api/users/${uid}/profile`,
            `${API_BASE}/api/users/${uid}`,
          ],
          { headers: { ...authHeaders() } },
        );

        if (dead) return;

        const rawP =
          data?.profile || data?.user_profile || data?.user || data || null;
        const p = normalizeProfile(rawP);

        const st = data?.stats || data?.profile_stats || null;

        setProfile(p);
        setStats(st);

        // ✅ marketplace ratings stats (for user's listings)
        try {
          const rr = await tryFetchFallback(
            [
              `${API_BASE}/api/users/${uid}/ratings-stats`,
              `${API_BASE}/api/user/${uid}/ratings-stats`,
            ],
            { headers: { ...authHeaders() } },
          );
          if (!dead) setRatingsStats(rr || null);
        } catch {
          if (!dead) setRatingsStats(null);
        }

        const meId = getAuthUserId();
        const fallbackIsMe = !!meId && normId(meId) === normId(uid);

        setIsMe(!!data?.isMe || fallbackIsMe);
        setIsFollowing(
          typeof data?.isFollowing === "boolean" ? data.isFollowing : false,
        );

        // ✅ EDIT FORM comes from SAME normalized profile
        setEditForm(toEditFormFromProfile(p));
      } catch (e) {
        toast.error(e.message || tt(lang, "failedLoadProfile"));
        setProfile(null);
        setStats(null);
        setRatingsStats(null);
      } finally {
        if (!dead) setLoading(false);
      }
    }

    load();
    return () => {
      dead = true;
    };
  }, [API_BASE, effectiveUserId, lang]);

  const refreshListingsAll = useCallback(async () => {
    if (!listingsOwnerId) {
      setListingsAll([]);
      setListingsLoading(false);
      return;
    }
    try {
      setListingsLoading(true);
      const all = await fetchUserListingsAll(API_BASE, listingsOwnerId);
      setListingsAll(Array.isArray(all) ? all : []);
    } catch {
      setListingsAll([]);
    } finally {
      setListingsLoading(false);
    }
  }, [API_BASE, listingsOwnerId]);

  // ✅ preload listings (counter + tab)
  useEffect(() => {
    refreshListingsAll();
  }, [refreshListingsAll]);

  // ✅ preload reviews for counters (so counts show immediately)
  // ✅ CVs count for owner (shows in tab like listings/reviews)
  useEffect(() => {
    if (!canEdit) {
      setCvCount(0);
      return;
    }

    let dead = false;

    (async () => {
      try {
        const r = await tryFetchFallback(
          [
            `${API_BASE}/api/cv`,
            `${API_BASE}/api/cvs`,
            `${API_BASE}/api/resumes`,
          ],
          { headers: { ...authHeaders() } },
        );

        const list = Array.isArray(r)
          ? r
          : Array.isArray(r?.cvs)
            ? r.cvs
            : Array.isArray(r?.data)
              ? r.data
              : Array.isArray(r?.items)
                ? r.items
                : [];

        if (!dead) setCvCount(list.length || 0);
      } catch {
        if (!dead) setCvCount(0);
      }
    })();

    return () => {
      dead = true;
    };
  }, [API_BASE, canEdit]);

  useEffect(() => {
    if (!effectiveUserId) return;
    let dead = false;

    (async () => {
      try {
        const uid = String(effectiveUserId || "").trim();
        if (!uid) return;

        const allListings = await fetchUserListingsAll(API_BASE, uid);
        const receivedRaw = await fetchReceivedReviewsViaListings(
          API_BASE,
          uid,
          allListings,
        );

        if (dead) return;
        setReviews(
          (Array.isArray(receivedRaw) ? receivedRaw : []).map(
            normalizeReviewForUI,
          ),
        );
      } catch {
        if (!dead) setReviews([]);
      }
    })();

    return () => {
      dead = true;
    };
  }, [API_BASE, effectiveUserId]);

  // ✅ refresh when user opens listings tab (always latest)
  useEffect(() => {
    if (tab === "listingsAll") refreshListingsAll();
  }, [tab, refreshListingsAll]);

  // ✅ refresh when coming back from /community (add/edit)
  useEffect(() => {
    const isFromCommunity =
      String(location?.state?.from || "") === "community" ||
      String(location?.search || "").includes("from=community") ||
      String(location?.search || "").includes("from=profile");

    const wasAddOrEdit =
      String(location?.search || "").includes("add=") ||
      String(location?.search || "").includes("edit=");

    if (isFromCommunity || wasAddOrEdit) {
      refreshListingsAll();
    }
  }, [location?.key, location?.search, location?.state, refreshListingsAll]);

  useEffect(() => {
    if (!effectiveUserId) return;
    let dead = false;

    async function loadTab() {
      setTabLoading(true);
      try {
        const uid = String(effectiveUserId || "").trim();
        if (!uid) return;

        if (tab === "posts") {
          const r = await tryFetchFallback(
            [
              `${API_BASE}/api/profile/${uid}/posts`,
              `${API_BASE}/api/profile_posts/${uid}`,
              `${API_BASE}/api/profile-posts/${uid}`,
              `${API_BASE}/api/users/${uid}/posts`,
              `${API_BASE}/api/posts?userId=${encodeURIComponent(uid)}`,
              `${API_BASE}/api/post?userId=${encodeURIComponent(uid)}`,
              `${API_BASE}/api/posts/user/${encodeURIComponent(uid)}`,
              `${API_BASE}/api/user/${encodeURIComponent(uid)}/posts`,
            ],
            { headers: { ...authHeaders() } },
          );
          const items =
            r?.posts ||
            r?.items ||
            r?.data ||
            r?.results ||
            (Array.isArray(r) ? r : []);
          if (!dead) {
            const arr = Array.isArray(items) ? items : [];
            setPosts(arr.map((x) => normalizePostForMedia(API_BASE, x)));
          }
        }

        if (tab === "reviews") {
          const uid = String(effectiveUserId || "").trim();

          // 1) optional stats
          let rStats = null;
          try {
            rStats = await tryFetchFallback(
              [
                `${API_BASE}/api/users/${uid}/ratings-stats`,
                `${API_BASE}/api/user/${uid}/ratings-stats`,
              ],
              { headers: { ...authHeaders() } },
            );
          } catch {
            rStats = null;
          }

          // 2) fetch owned listings then fetch reviews per listing
          const allListings = await fetchUserListingsAll(API_BASE, uid);

          const receivedRaw = await fetchReceivedReviewsViaListings(
            API_BASE,
            uid,
            allListings,
          );

          const receivedUI = receivedRaw.map(normalizeReviewForUI);

          if (!dead) {
            const count = receivedUI.length;
            const avg =
              count > 0
                ? receivedUI.reduce((s, x) => s + (Number(x?.stars) || 0), 0) /
                  count
                : 0;

            setRatingsStats(
              rStats || {
                reviews_count: count,
                avg_rating: avg,
              },
            );
            setReviews(receivedUI);
          }
        }
      } catch (e) {
        toast.error(e.message || tt(lang, "failedLoadTab"));
        if (!dead) {
          if (tab === "posts") setPosts([]);
          if (tab === "reviews") setReviews([]);
        }
      } finally {
        if (!dead) setTabLoading(false);
      }
    }

    loadTab();
    return () => {
      dead = true;
    };
  }, [API_BASE, effectiveUserId, tab, lang]);

  async function onUploadCover(file) {
    if (!canEdit) return toast.error(tt(lang, "loginFirst"));
    if (!file) return;

    const fd = new FormData();
    fd.append("cover", file);

    try {
      const r = await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/cover`,
          `${API_BASE}/api/profile/cover`,
          `${API_BASE}/api/me/profile/cover`,
          `${API_BASE}/api/user/profile/me/cover`,
        ],
        { method: "POST", headers: { ...authHeaders() }, body: fd },
      );

      const nextUrl =
        r?.cover_url ||
        r?.url ||
        r?.profile?.cover_url ||
        r?.user_profile?.cover_url ||
        "";

      // ✅ update SAME normalized profile
      setProfile((prev) =>
        normalizeProfile({ ...(prev || {}), cover_url: nextUrl }),
      );
      setEditForm((f) => ({ ...f, cover_url: nextUrl }));

      toast.success(tt(lang, "saved"));
    } catch (e) {
      toast.error(e.message || "Cover upload failed (backend route?)");
    }
  }

  async function onDeleteCover() {
    if (!canEdit) return toast.error(tt(lang, "loginFirst"));

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/cover`,
          `${API_BASE}/api/profile/cover`,
          `${API_BASE}/api/me/profile/cover`,
          `${API_BASE}/api/user/profile/me/cover`,
        ],
        { method: "DELETE", headers: { ...authHeaders() } },
      );

      setProfile((prev) =>
        normalizeProfile({ ...(prev || {}), cover_url: "" }),
      );
      setEditForm((f) => ({ ...f, cover_url: "" }));

      toast.success(tt(lang, "deleted"));
    } catch (e) {
      toast.error(e.message || "Delete cover failed (backend route?)");
    }
  }

  async function onUploadAvatar(file) {
    if (!canEdit) return toast.error(tt(lang, "loginFirst"));
    if (!file) return;

    const fd = new FormData();
    fd.append("avatar", file);

    try {
      const r = await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/avatar`,
          `${API_BASE}/api/profile/avatar`,
          `${API_BASE}/api/me/profile/avatar`,
          `${API_BASE}/api/user/profile/me/avatar`,
        ],
        { method: "POST", headers: { ...authHeaders() }, body: fd },
      );

      const nextUrl =
        r?.avatar_url ||
        r?.url ||
        r?.profile?.avatar_url ||
        r?.user_profile?.avatar_url ||
        "";

      setProfile((prev) =>
        normalizeProfile({ ...(prev || {}), avatar_url: nextUrl }),
      );
      setEditForm((f) => ({ ...f, avatar_url: nextUrl }));

      toast.success(tt(lang, "saved"));
    } catch (e) {
      toast.error(e.message || "Avatar upload failed");
    }
  }

  async function onDeleteAvatar() {
    if (!canEdit) return toast.error(tt(lang, "loginFirst"));

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/avatar`,
          `${API_BASE}/api/profile/avatar`,
          `${API_BASE}/api/me/profile/avatar`,
          `${API_BASE}/api/user/profile/me/avatar`,
        ],
        { method: "DELETE", headers: { ...authHeaders() } },
      );

      setProfile((prev) =>
        normalizeProfile({ ...(prev || {}), avatar_url: "" }),
      );
      setEditForm((f) => ({ ...f, avatar_url: "" }));

      toast.success(tt(lang, "deleted"));
    } catch (e) {
      toast.error(e.message || "Delete avatar failed");
    }
  }

  async function onFollowToggle() {
    if (!canAct) return toast.error(tt(lang, "loginToFollow"));
    try {
      if (isFollowing) {
        await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${effectiveUserId}/follow`,
            `${API_BASE}/api/follow/${effectiveUserId}`,
          ],
          {
            method: "DELETE",
            headers: { "Content-Type": "application/json", ...authHeaders() },
          },
        );
        setIsFollowing(false);
        setStats((s) =>
          s ? { ...s, followers: Math.max(0, (s.followers || 0) - 1) } : s,
        );
      } else {
        await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${effectiveUserId}/follow`,
            `${API_BASE}/api/follow/${effectiveUserId}`,
          ],
          {
            method: "POST",
            headers: { "Content-Type": "application/json", ...authHeaders() },
          },
        );
        setIsFollowing(true);
        setStats((s) => (s ? { ...s, followers: (s.followers || 0) + 1 } : s));
      }
    } catch (e) {
      toast.error(e.message || tt(lang, "followFailed"));
    }
  }

  async function onSaveProfile() {
    if (!canEdit) return;

    // ✅ optimistic: update displayed profile from SAME editForm immediately
    setProfile((prev) => mergeProfileWithEditForm(prev, editForm));

    try {
      const payload = buildProfileSavePayload(editForm);

      const r = await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me`,
          `${API_BASE}/api/me/profile`,
          `${API_BASE}/api/user/profile/me`,
          // extra fallbacks
          `${API_BASE}/api/users/me`,
          `${API_BASE}/api/user/me`,
        ],
        {
          method: "PUT",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify(payload),
        },
      );

      toast.success(tt(lang, "saved"));

      const nextRaw = r?.profile || r?.user_profile || r?.user || r || {};
      const next = normalizeProfile(nextRaw);

      setProfile(next);
      setEditForm(toEditFormFromProfile(next));

      setEditOpen(false);
      // ✅ ensure we reflect DB truth after save (same source as display)
      try {
        const uid = String(effectiveUserId || "").trim();
        if (uid) {
          const data = await tryFetchFallback(
            [
              `${API_BASE}/api/profile/${uid}`,
              `${API_BASE}/api/profiles/${uid}`,
              `${API_BASE}/api/user/${uid}/profile`,
              `${API_BASE}/api/users/${uid}/profile`,
              `${API_BASE}/api/users/${uid}`,
            ],
            { headers: { ...authHeaders() } },
          );
          const rawP =
            data?.profile || data?.user_profile || data?.user || data || null;
          const p = normalizeProfile(rawP);
          setProfile(p);
          setEditForm(toEditFormFromProfile(p));
        }
      } catch {}
    } catch (e) {
      toast.error(e.message || tt(lang, "saveFailed"));
    }
  }

  async function refreshCurrentTab() {
    try {
      const uid = String(effectiveUserId || "").trim();
      if (!uid) return;

      if (tab === "posts") {
        const r = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}/posts`,
            `${API_BASE}/api/profile_posts/${uid}`,
            `${API_BASE}/api/profile-posts/${uid}`,
            `${API_BASE}/api/users/${uid}/posts`,
            `${API_BASE}/api/posts?userId=${encodeURIComponent(uid)}`,
            `${API_BASE}/api/posts/user/${encodeURIComponent(uid)}`,
          ],
          { headers: { ...authHeaders() } },
        );
        const items =
          r?.posts ||
          r?.items ||
          r?.data ||
          r?.results ||
          (Array.isArray(r) ? r : []);
        const arr = Array.isArray(items) ? items : [];
        setPosts(arr.map((x) => normalizePostForMedia(API_BASE, x)));
      }

      if (tab === "reviews") {
        const rStats = await tryFetchFallback(
          [
            `${API_BASE}/api/users/${uid}/ratings-stats`,
            `${API_BASE}/api/user/${uid}/ratings-stats`,
          ],
          { headers: { ...authHeaders() } },
        ).catch(() => null);

        const allListings = await fetchUserListingsAll(API_BASE, uid);

        const receivedRaw = await fetchReceivedReviewsViaListings(
          API_BASE,
          uid,
          allListings,
        );

        const receivedUI = receivedRaw.map(normalizeReviewForUI);

        const count = receivedUI.length;
        const avg =
          count > 0
            ? receivedUI.reduce((s, x) => s + (Number(x?.stars) || 0), 0) /
              count
            : 0;

        setRatingsStats(
          rStats || {
            reviews_count: count,
            avg_rating: avg,
          },
        );
        setReviews(receivedUI);
      }

      if (tab === "listingsAll") {
        await refreshListingsAll();
      }
    } catch {}
  }

  /* =========================
     ✅ CREATE REVIEW (THIS FIXES "اكتب تقييم")
  ========================= */
  async function onCreateReview() {
    if (!canAct) return toast.error(tt(lang, "loginFirst") || "Login first");

    const uid = String(effectiveUserId || "").trim();
    if (!uid) return toast.error("Missing userId");
    if (computedIsMe) return toast.error("مينفعش تقيم نفسك");

    const ratingNum = Number(reviewForm?.rating ?? 0);
    const rating = Math.max(
      1,
      Math.min(5, Number.isFinite(ratingNum) ? ratingNum : 5),
    );
    const comment = String(reviewForm?.comment ?? "").trim();

    const payload = {
      rating,
      stars: rating,
      value: rating,
      comment,
      text: comment,
      body: comment,
      message: comment,
      target_user_id: Number(uid),
      user_id: Number(uid),
    };

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/users/${uid}/reviews`,
          `${API_BASE}/api/user/${uid}/reviews`,
          `${API_BASE}/api/users/${uid}/ratings`,
          `${API_BASE}/api/user/${uid}/ratings`,
          `${API_BASE}/api/profile/${uid}/reviews`,
          `${API_BASE}/api/profile/${uid}/ratings`,
          `${API_BASE}/api/ratings/user/${uid}`,
          `${API_BASE}/api/reviews/user/${uid}`,
        ],
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify(payload),
        },
      );

      toast.success("تم إرسال التقييم");
      setAddReviewOpen(false);
      setReviewForm({ rating: 5, comment: "" });

      setTab("reviews");
      await refreshCurrentTab();
    } catch (e) {
      toast.error(e.message || "فشل إرسال التقييم (راجع endpoint في الباك)");
    }
  }

  async function onDeletePost(postId) {
    if (!canEdit) return;

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deletePostQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    const target = normId(postId);
    const prev = posts;

    setPosts((xs) =>
      (Array.isArray(xs) ? xs : []).filter(
        (p) => normId(getPostId(p)) !== target,
      ),
    );

    try {
      const idForUrl = extractNumericId(postId) ?? postId;

      await tryFetchFallback(
        [
          `${API_BASE}/api/posts/${idForUrl}`,
          `${API_BASE}/api/post/${idForUrl}`,
          `${API_BASE}/api/profile/me/posts/${idForUrl}`,
          `${API_BASE}/api/me/profile/posts/${idForUrl}`,
          `${API_BASE}/api/profile_posts/me/${idForUrl}`,
        ],
        { method: "DELETE", headers: { ...authHeaders() } },
      );

      toast.success(tt(lang, "deleted"));
      setStats((s) =>
        s ? { ...s, posts: Math.max(0, (s.posts || 0) - 1) } : s,
      );
      await refreshCurrentTab();
    } catch (e) {
      setPosts(prev);
      toast.error(e.message || tt(lang, "deleteFailed"));
    }
  }

  async function onUpdatePost(postId, payload) {
    if (!canEdit) return;

    const content = String(
      payload?.content ?? payload?.text ?? payload?.body ?? "",
    ).trim();
    if (!content) return toast.error(tt(lang, "postContentRequired"));

    const keepMedia = uniq([
      ...toArr(payload?.keepMedia),
      ...toArr(payload?.keep_media),
      ...toArr(payload?.keep_media_urls),
      ...toArr(payload?.media_keep),
      ...toArr(payload?.mediaKeep),
      ...toArr(payload?.media),
    ]);

    const removeMedia = uniq([
      ...toArr(payload?.removeMedia),
      ...toArr(payload?.remove_media),
      ...toArr(payload?.remove_media_urls),
      ...toArr(payload?.media_remove),
      ...toArr(payload?.mediaRemove),
    ]);

    const files =
      payload?.files ||
      payload?.newFiles ||
      payload?.images ||
      payload?.photos ||
      payload?.mediaFiles ||
      payload?.media_files ||
      [];

    const hasMediaOps =
      keepMedia.length ||
      removeMedia.length ||
      (Array.isArray(files) ? files.length : files?.length || 0);

    const prev = posts;
    const target = normId(postId);

    setPosts((xs) =>
      (Array.isArray(xs) ? xs : []).map((p) => {
        const pid = normId(getPostId(p));
        if (pid !== target) return p;
        const next = { ...p, content };
        if (Array.isArray(payload?.media)) next.media = payload.media;
        return next;
      }),
    );

    const idForUrl = extractNumericId(postId) ?? postId;

    const urls = [
      `${API_BASE}/api/posts/${idForUrl}`,
      `${API_BASE}/api/post/${idForUrl}`,
      `${API_BASE}/api/profile/me/posts/${idForUrl}`,
      `${API_BASE}/api/me/profile/posts/${idForUrl}`,
    ];

    try {
      if (hasMediaOps) {
        const fd = buildUpdateFormData({
          content,
          keepMedia,
          removeMedia,
          files,
        });

        try {
          await tryFetchFallback(urls, {
            method: "PUT",
            headers: { ...authHeaders() },
            body: fd,
          });
        } catch {
          await tryFetchFallback(urls, {
            method: "PATCH",
            headers: { ...authHeaders() },
            body: fd,
          });
        }
      } else {
        const body = JSON.stringify({
          content,
          text: content,
          body: content,
          message: content,
        });

        try {
          await tryFetchFallback(urls, {
            method: "PUT",
            headers: { "Content-Type": "application/json", ...authHeaders() },
            body,
          });
        } catch {
          await tryFetchFallback(urls, {
            method: "PATCH",
            headers: { "Content-Type": "application/json", ...authHeaders() },
            body,
          });
        }
      }

      toast.success(tt(lang, "updated"));
      await refreshCurrentTab();
    } catch (e) {
      setPosts(prev);
      toast.error(e.message || tt(lang, "updateFailed"));
    }
  }

  const onAddListingClick = () => {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));
    if (!computedIsMe) return toast.error(tt(lang, "notAllowed"));

    navigate(
      `/community?add=1&from=profile&uid=${encodeURIComponent(
        String(listingsOwnerId || ""),
      )}`,
    );
  };

  const onEditListing = (item) => {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));

    const type = String(item?._type || item?.type || "").toLowerCase();
    const id = getRawId(item);
    const pref = buildPrefixedId(type, id);
    if (!pref) return navigate("/community");

    navigate(
      `/community?edit=1&id=${encodeURIComponent(
        pref,
      )}&from=profile&uid=${encodeURIComponent(String(listingsOwnerId || ""))}`,
    );
  };

  const onDeleteListing = async (item) => {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));

    const type = String(item?._type || item?.type || "").toLowerCase();
    const id = getRawId(item);
    const pref = buildPrefixedId(type, id);
    if (!pref) return;

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    const prev = listingsAll;
    setListingsAll((xs) =>
      (Array.isArray(xs) ? xs : []).filter((x) => x !== item),
    );

    try {
      if (type === "places") {
        const onlyNum = String(pref).split("_")[1];
        await tryFetchFallback(
          [
            `${API_BASE}/api/community/places/${onlyNum}`,
            `${API_BASE}/api/places/${onlyNum}`,
          ],
          { method: "DELETE", headers: { ...authHeaders() } },
        );
      } else if (type === "groups") {
        const onlyNum = String(pref).split("_")[1];
        await tryFetchFallback(
          [
            `${API_BASE}/api/community/groups/${onlyNum}`,
            `${API_BASE}/api/groups/${onlyNum}`,
          ],
          { method: "DELETE", headers: { ...authHeaders() } },
        );
      } else {
        await tryFetchFallback(
          [
            `${API_BASE}/api/listings/${pref}`,
            `${API_BASE}/api/listings/${encodeURIComponent(pref)}`,
          ],
          { method: "DELETE", headers: { ...authHeaders() } },
        );
      }

      toast.success(tt(lang, "deleted"));
      await refreshListingsAll();
    } catch (e) {
      setListingsAll(prev);
      toast.error(e.message || tt(lang, "deleteFailed"));
    }
  };

  const ratingAvg = useMemo(() => {
    const arr = Array.isArray(reviews) ? reviews : [];
    const n = arr.length;
    if (!n) return 0;
    const sum = arr.reduce(
      (s, r) => s + (Number(r?.stars ?? r?.rating ?? 0) || 0),
      0,
    );
    return sum / n;
  }, [reviews]);

  const followers = Number(stats?.followers ?? 0) || 0;
  const following = Number(stats?.following ?? 0) || 0;

  const safeProfile = useMemo(() => normalizeProfile(profile || {}), [profile]);

  const cover = absUrl(API_BASE, safeProfile.cover_url || "");
  const avatar = absUrl(API_BASE, safeProfile.avatar_url || "");

  const displayName =
    safeProfile.display_name || safeProfile.username || "User";
  const username = safeProfile.username ? `@${safeProfile.username}` : "";
  const verified = !!safeProfile.is_verified;

  // ✅ always pass normalized reviews to body
  const reviewsUI = useMemo(
    () => (Array.isArray(reviews) ? reviews : []).map(normalizeReviewForUI),
    [reviews],
  );

  async function onMessageUser() {
    if (!canAct) return toast.error(tt(lang, "loginFirst") || "Login first");

    const otherId = Number(String(effectiveUserId || "").trim());

    if (!Number.isFinite(otherId) || otherId <= 0) return;
    if (computedIsMe) return;

    try {
      const r = await tryFetchFallback([`${API_BASE}/api/chat/threads`], {
        method: "POST",
        headers: { "Content-Type": "application/json", ...authHeaders() },
        body: JSON.stringify({
          other_user_id: otherId,
          context_type: "profile",
          context_id: String(otherId),
          context_label: displayName,
        }),
      });

      const thread = r?.thread || r?.data?.thread || r?.item || r;

      window.dispatchEvent(
        new CustomEvent("a4u:chat-open", { detail: { thread } }),
      );
    } catch (e) {
      toast.error(e.message || "Chat failed");
    }
  }

  return (
    <>
      <ProfilePageBody
        lang={lang}
        dir={dir}
        API_BASE={API_BASE}
        navigate={navigate}
        loading={loading}
        profile={safeProfile}
        stats={stats}
        isFollowing={isFollowing}
        canEdit={canEdit}
        canAct={canAct}
        tab={tab}
        setTab={setTab}
        tabLoading={tabLoading}
        posts={posts}
        reviews={reviewsUI}
        countPosts={countPosts}
        countReviews={countReviews}
        ratingAvg={ratingAvg}
        followers={followers}
        following={following}
        cover={cover}
        avatar={avatar}
        displayName={displayName}
        username={username}
        verified={verified}
        cvCount={cvCount}
        onShare={() => {
          const url = window.location.href;
          const text = `Profile on AnswerForU: ${displayName}`;
          (async () => {
            try {
              if (navigator.share) {
                await navigator.share({ title: "AnswerForU", text, url });
                toast.success(tt(lang, "shared"));
                return;
              }
              await navigator.clipboard.writeText(url);
              toast.success(tt(lang, "linkCopied"));
            } catch {
              try {
                await navigator.clipboard.writeText(url);
                toast.success(tt(lang, "linkCopied"));
              } catch {
                toast("Copy link: " + url);
              }
            }
          })();
        }}
        onFollowToggle={onFollowToggle}
        onDeletePost={onDeletePost}
        onUpdatePost={onUpdatePost}
        refreshCurrentTab={refreshCurrentTab}
        onSaveProfile={onSaveProfile}
        onCreateReview={onCreateReview}
        editOpen={editOpen}
        setEditOpen={setEditOpen}
        addReviewOpen={addReviewOpen}
        setAddReviewOpen={setAddReviewOpen}
        editForm={editForm}
        setEditForm={setEditForm}
        reviewForm={reviewForm}
        setReviewForm={setReviewForm}
        PostCardComp={PostCard}
        PostComposerComp={PostComposer}
        onUploadAvatar={onUploadAvatar}
        onDeleteAvatar={onDeleteAvatar}
        onUploadCover={onUploadCover}
        onDeleteCover={onDeleteCover}
        listingsAll={listingsAll}
        listingsLoading={listingsLoading}
        countListingsAll={countListingsAll}
        onAddListingClick={onAddListingClick}
        onEditListing={onEditListing}
        onDeleteListing={onDeleteListing}
        listingsOwnerId={listingsOwnerId}
      />
    </>
  );
}

export default ProfilePage;
