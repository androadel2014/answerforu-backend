// src/components/ProfilePageBody.jsx
import React, { useMemo } from "react";
import { Link } from "react-router-dom";
import toast from "react-hot-toast";
import {
  MapPin,
  Link as LinkIcon,
  Phone,
  BadgeCheck,
  Plus,
  Pencil,
  Star,
  Store,
  MessageCircle,
  Share2,
} from "lucide-react";

import { CardItem } from "../community/CardItem";

import {
  tt,
  classNames,
  safeUrl,
  getInitials,
  absUrl,
  Modal,
  StatsPanel,
  TabPill,
  Field,
  PostsTab,
  ReviewsTab,
} from "./profilePage.parts";

function fmtWhen(v) {
  const s = String(v || "").trim();
  if (!s) return "";
  const d = new Date(s);
  if (!Number.isFinite(d.getTime())) return s;
  return d.toLocaleString();
}

function StarsRow({ value = 0 }) {
  const n = Math.max(0, Math.min(5, Number(value) || 0));
  return (
    <div className="flex items-center gap-1">
      {Array.from({ length: 5 }).map((_, i) => {
        const on = i < n;
        return (
          <Star
            key={i}
            size={18}
            className={on ? "text-yellow-500" : "text-gray-300"}
            fill={on ? "currentColor" : "none"}
          />
        );
      })}
    </div>
  );
}

function pickReviewName(r) {
  const name =
    r?.reviewer_name ||
    r?.user_name ||
    r?.userName ||
    r?.username ||
    r?.email ||
    "";
  return String(name || "").trim() || "User";
}

function pickReviewUserId(r) {
  const id = Number(
    r?.reviewer_id ?? r?.reviewerId ?? r?.user_id ?? r?.userId ?? 0
  );
  return Number.isFinite(id) && id > 0 ? id : 0;
}

function pickItemTitle(r) {
  const t =
    r?.item_title ||
    r?.itemTitle ||
    r?.title ||
    r?.listing_title ||
    r?.listingTitle ||
    "";
  return String(t || "").trim();
}

function pickItemHref(r) {
  // coming from ProfilePage normalize: item_href
  const href = String(r?.item_href || "").trim();
  if (href) return href;

  // fallback: build from item_id
  const id = Number(r?.item_id ?? r?.itemId ?? 0);
  if (Number.isFinite(id) && id > 0) return `/marketplace/item/${id}`;
  return "";
}

/* =========================
   Adapter: listing -> CardItem shape (minimal)
   + keep prefixed id for /marketplace/item/:id
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

function toCardItemShape(raw) {
  const it = raw || {};
  const type = String(it?._type || it?.type || "").toLowerCase() || "services";

  const idNum =
    it?.id ??
    it?.listing_id ??
    it?.service_id ??
    it?.product_id ??
    it?.job_id ??
    it?.housing_id ??
    null;

  const pref = buildPrefixedId(type, idNum);

  return {
    // IMPORTANT: CommunityView uses prefixed ids in unified items
    // CardItem doesn't require id format, but onOpen needs it for route
    id: pref || idNum,

    type, // used when tab === "all" in CardItem
    name: it?.name || it?.title || it?.headline || "Untitled",
    title: it?.title || it?.name || it?.headline || "Untitled",

    category: it?.category || it?.place_category || it?.service_category || "",
    platform: it?.platform || it?.group_platform || "",
    topic: it?.topic || it?.group_topic || "",

    state: it?.state || "",
    city: it?.city || "",
    address: it?.address || "",

    website: it?.website || it?.site || it?.url || "",
    phone: it?.phone || it?.tel || "",
    link: it?.link || it?.url || it?.website || "",

    contact: it?.contact || it?.phone || it?.whatsapp || it?.email || "",

    description: it?.description || it?.notes || it?.desc || "",

    price_value:
      it?.price_value ??
      it?.priceValue ??
      it?.price ??
      it?.amount ??
      it?.budget ??
      null,

    createdAt:
      it?.createdAt ||
      it?.created_at ||
      it?.updatedAt ||
      it?.updated_at ||
      null,
    created_at: it?.created_at || it?.createdAt || null,

    avg_rating:
      it?.avg_rating ??
      it?.rating_avg ??
      it?.avgRating ??
      it?.rating ??
      it?.stars ??
      0,

    reviews_count:
      it?.reviews_count ??
      it?.review_count ??
      it?.reviewsCount ??
      it?.rating_count ??
      it?.count ??
      0,

    created_by:
      it?.created_by ??
      it?.createdBy ??
      it?.user_id ??
      it?.userId ??
      it?.owner_id ??
      it?.ownerId ??
      0,
  };
}

export default function ProfilePageBody({
  // basics
  lang,
  dir,
  API_BASE,
  navigate,
  onUploadCover,
  onDeleteCover,

  // data
  loading,
  profile,
  stats,
  isFollowing,
  canEdit,
  canAct,

  // tab data
  tab,
  setTab,
  tabLoading,
  posts,
  reviews,

  // counts
  countPosts,
  countReviews,

  // derived UI
  ratingAvg,
  followers,
  following,
  cover,
  avatar,
  displayName,
  username,
  verified,

  // handlers
  onShare,
  onFollowToggle,
  onDeletePost,
  onUpdatePost,
  refreshCurrentTab,

  onSaveProfile,
  onCreateReview,

  // avatar
  onUploadAvatar,
  onDeleteAvatar,

  // modals state
  editOpen,
  setEditOpen,
  addReviewOpen,
  setAddReviewOpen,

  // forms
  editForm,
  setEditForm,
  reviewForm,
  setReviewForm,

  // components (injected)
  PostCardComp,
  PostComposerComp,

  // listings
  listingsAll = [],
  listingsLoading = false,
  countListingsAll = 0,
  onAddListingClick,
  onEditListing,
  onDeleteListing,
  listingsOwnerId,
}) {
  if (loading) {
    return (
      <div className="max-w-5xl mx-auto p-4 md:p-6" dir={dir}>
        <div className="animate-pulse space-y-4">
          <div className="h-44 rounded-2xl bg-gray-200" />
          <div className="h-20 rounded-2xl bg-gray-200" />
          <div className="h-64 rounded-2xl bg-gray-200" />
        </div>
      </div>
    );
  }

  if (!profile) {
    return (
      <div className="max-w-5xl mx-auto p-6" dir={dir}>
        <div className="bg-white border rounded-2xl p-6">
          <div className="font-semibold mb-2">
            {tt(lang, "profileNotFound")}
          </div>
          <button
            onClick={() => navigate("/")}
            className="px-4 py-2 rounded-xl bg-black text-white"
          >
            {tt(lang, "back")}
          </button>
        </div>
      </div>
    );
  }

  const canAvatarActions = !!canEdit && !!onUploadAvatar && !!onDeleteAvatar;
  const coverSrcRaw = cover || profile.cover_url || "";
  const coverSrc = coverSrcRaw ? absUrl(API_BASE, coverSrcRaw) : "";

  const marketplaceLabel =
    lang === "ar" ? "إعلاناتي" : lang === "es" ? "Mis anuncios" : "My Listings";

  const showAddForListings =
    tab === "listingsAll" && !!canAct && !!listingsOwnerId;

  const onAddListing = () => {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));
    if (typeof onAddListingClick === "function") return onAddListingClick();
    navigate("/community?add=1");
  };

  // ✅ Build cards for CardItem
  const cardRows = useMemo(() => {
    const arr = Array.isArray(listingsAll) ? listingsAll : [];
    return arr.map((raw) => {
      const t = String(raw?._type || raw?.type || "services").toLowerCase();
      const it = toCardItemShape(raw);

      // ensure prefixed id for route
      const prefId = String(it?.id || "").includes("_")
        ? String(it.id)
        : buildPrefixedId(t, it?.id);

      return { raw, t, it: { ...it, id: prefId || it.id } };
    });
  }, [listingsAll]);

  return (
    <div className="max-w-5xl mx-auto p-4 md:p-6" dir={dir}>
      {/* Header */}
      <div className="relative rounded-2xl overflow-hidden border bg-white">
        <div className="relative">
          <div className="w-full h-44 md:h-56 bg-gradient-to-r from-gray-100 to-gray-200" />
          {coverSrc ? (
            <img
              src={coverSrc}
              alt="cover"
              className="absolute inset-0 w-full h-44 md:h-56 object-cover"
              onError={(e) => {
                e.currentTarget.style.display = "none";
              }}
            />
          ) : null}

          {canEdit && onUploadCover && onDeleteCover ? (
            <div className="absolute top-3 right-3 flex gap-2">
              <label className="cursor-pointer select-none">
                <input
                  type="file"
                  accept="image/*"
                  className="hidden"
                  onChange={(e) => {
                    const f = e.target.files?.[0];
                    if (!f) return;
                    onUploadCover(f);
                    e.target.value = "";
                  }}
                />
                <div className="px-3 py-2 rounded-xl bg-black/85 text-white hover:bg-black flex items-center gap-2">
                  <Pencil size={16} />
                  {coverSrc ? tt(lang, "change") : tt(lang, "add")}
                </div>
              </label>

              <button
                type="button"
                onClick={() => {
                  if (!coverSrc) return;
                  onDeleteCover();
                }}
                className={classNames(
                  "px-3 py-2 rounded-xl border flex items-center justify-center",
                  coverSrc
                    ? "bg-white/90 hover:bg-white text-red-600"
                    : "bg-white/60 text-gray-400 cursor-not-allowed"
                )}
              >
                {tt(lang, "delete")}
              </button>
            </div>
          ) : null}
        </div>

        <div className="p-4 md:p-6">
          <div className="flex flex-col md:flex-row md:items-end gap-4">
            <div className="-mt-12 md:-mt-16 flex items-end gap-4">
              <div className="relative">
                <div className="w-24 h-24 md:w-28 md:h-28 rounded-2xl overflow-hidden border bg-white shadow">
                  {avatar ? (
                    <img
                      src={avatar}
                      alt="avatar"
                      className="w-full h-full object-cover"
                      onError={(e) => {
                        e.currentTarget.style.display = "none";
                      }}
                    />
                  ) : (
                    <div className="w-full h-full flex items-center justify-center bg-gray-900 text-white text-2xl font-extrabold">
                      {getInitials(displayName)}
                    </div>
                  )}
                </div>

                {canAvatarActions ? (
                  <div className="absolute -bottom-2 -right-2 flex gap-2">
                    <label className="cursor-pointer select-none">
                      <input
                        type="file"
                        accept="image/*"
                        className="hidden"
                        onChange={(e) => {
                          const f = e.target.files?.[0];
                          if (!f) return;
                          onUploadAvatar(f);
                          e.target.value = "";
                        }}
                      />
                      <div className="w-9 h-9 rounded-xl bg-black text-white flex items-center justify-center shadow hover:bg-gray-900">
                        <Pencil size={16} />
                      </div>
                    </label>

                    <button
                      type="button"
                      onClick={() => onDeleteAvatar()}
                      className="w-9 h-9 rounded-xl bg-white border flex items-center justify-center shadow hover:bg-gray-50"
                      title={tt(lang, "delete")}
                    >
                      <span className="text-red-600 text-lg leading-none">
                        ×
                      </span>
                    </button>
                  </div>
                ) : null}
              </div>

              <div className="pb-1">
                <div className="flex items-center gap-2">
                  <div className="text-xl md:text-2xl font-bold">
                    {displayName}
                  </div>
                  {verified ? (
                    <BadgeCheck className="text-blue-600" size={20} />
                  ) : null}
                </div>
                <div className="text-sm text-gray-500">{username}</div>
              </div>
            </div>

            <div
              className={classNames(
                "md:ml-auto flex flex-wrap gap-2",
                lang === "ar" ? "md:ml-0 md:mr-auto" : ""
              )}
            >
              <button
                onClick={onShare}
                className="px-4 py-2 rounded-xl border bg-white hover:bg-gray-50 flex items-center gap-2"
              >
                <Share2 size={16} />
                {tt(lang, "share")}
              </button>

              {!canEdit ? (
                <button
                  onClick={onFollowToggle}
                  className={classNames(
                    "px-4 py-2 rounded-xl flex items-center gap-2",
                    isFollowing
                      ? "bg-white border hover:bg-gray-50"
                      : "bg-black text-white hover:bg-gray-900"
                  )}
                >
                  <MessageCircle size={16} />
                  {isFollowing ? tt(lang, "following") : tt(lang, "follow")}
                </button>
              ) : (
                <button
                  onClick={() => setEditOpen(true)}
                  className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
                >
                  <Pencil size={16} />
                  {tt(lang, "editProfile")}
                </button>
              )}
            </div>
          </div>

          <div className="mt-4 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="md:col-span-2">
              <div className="text-gray-800 whitespace-pre-wrap">
                {profile.bio || (
                  <span className="text-gray-400">{tt(lang, "noBio")}</span>
                )}
              </div>

              <div className="mt-3 flex flex-wrap gap-3 text-sm text-gray-600">
                {profile.location ? (
                  <div className="flex items-center gap-2">
                    <MapPin size={16} /> {profile.location}
                  </div>
                ) : null}

                {profile.website ? (
                  <a
                    href={safeUrl(profile.website)}
                    target="_blank"
                    rel="noreferrer"
                    className="flex items-center gap-2 hover:underline"
                  >
                    <LinkIcon size={16} /> {tt(lang, "website")}
                  </a>
                ) : null}

                {profile.phone ? (
                  <div className="flex items-center gap-2">
                    <Phone size={16} /> {profile.phone}
                  </div>
                ) : null}
              </div>
            </div>

            <StatsPanel
              lang={lang}
              ratingAvg={ratingAvg}
              ratingCount={countReviews}
              followers={followers}
              following={following}
              posts={countPosts}
              services={0}
              products={0}
              myListingsCount={countListingsAll}
            />
          </div>
        </div>
      </div>

      {/* Tabs */}
      <div className="mt-4 bg-white border rounded-2xl overflow-hidden">
        <div className="flex flex-wrap items-center gap-2 px-3 py-3 border-b bg-white">
          <TabPill
            active={tab === "posts"}
            onClick={() => setTab("posts")}
            icon={<MessageCircle size={16} />}
            label={`${tt(lang, "posts")} (${countPosts})`}
          />

          <TabPill
            active={tab === "listingsAll"}
            onClick={() => setTab("listingsAll")}
            icon={<Store size={16} />}
            label={`${marketplaceLabel} (${countListingsAll})`}
          />

          <TabPill
            active={tab === "reviews"}
            onClick={() => setTab("reviews")}
            icon={<Star size={16} />}
            label={`${tt(lang, "reviews")} (${countReviews})`}
          />

          <div
            className={classNames(
              "ml-auto flex flex-wrap gap-2",
              lang === "ar" ? "ml-0 mr-auto" : ""
            )}
          >
            {showAddForListings ? (
              <button
                onClick={onAddListing}
                className="px-3 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
              >
                <Plus size={16} />
                {lang === "ar"
                  ? "إضافة إعلان"
                  : lang === "es"
                  ? "Añadir anuncio"
                  : "Add Listing"}
              </button>
            ) : null}

            {!canEdit && tab === "reviews" ? (
              <button
                onClick={() =>
                  canAct
                    ? setAddReviewOpen(true)
                    : toast.error(tt(lang, "loginFirst"))
                }
                className="px-3 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
              >
                <Star size={16} />
                {tt(lang, "writeReview")}
              </button>
            ) : null}
          </div>
        </div>

        <div className="p-4">
          {tabLoading ? (
            <div className="animate-pulse space-y-3">
              <div className="h-16 bg-gray-100 rounded-2xl" />
              <div className="h-16 bg-gray-100 rounded-2xl" />
              <div className="h-16 bg-gray-100 rounded-2xl" />
            </div>
          ) : null}

          {!tabLoading && tab === "posts" ? (
            <PostsTab
              lang={lang}
              API_BASE={API_BASE}
              profile={profile}
              items={posts}
              isMe={canEdit}
              canAct={canAct}
              onDelete={onDeletePost}
              onUpdate={onUpdatePost}
              refreshCurrentTab={refreshCurrentTab}
              PostCardComp={PostCardComp}
              PostComposerComp={PostComposerComp}
            />
          ) : null}

          {/* ✅ Listings: open/edit/delete exactly like Marketplace */}
          {!tabLoading && tab === "listingsAll" ? (
            <div className="space-y-3">
              {listingsLoading ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {Array.from({ length: 6 }).map((_, i) => (
                    <div
                      key={i}
                      className="animate-pulse border rounded-2xl bg-white p-4"
                    >
                      <div className="h-10 bg-gray-100 rounded-2xl" />
                      <div className="mt-3 h-5 bg-gray-100 rounded w-2/3" />
                      <div className="mt-2 h-4 bg-gray-100 rounded w-1/2" />
                      <div className="mt-2 h-4 bg-gray-100 rounded w-4/5" />
                    </div>
                  ))}
                </div>
              ) : cardRows.length ? (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  {cardRows.map(({ raw, t, it }, idx) => {
                    const openType = t;
                    const openId = String(it?.id || "");
                    return (
                      <CardItem
                        key={`${t}_${openId || idx}`}
                        lang={lang}
                        tab={t}
                        it={it}
                        isLoggedIn={!!canAct}
                        onEdit={() => onEditListing?.(raw)}
                        onDelete={() => onDeleteListing?.(raw)}
                        onOpen={() => {
                          // ✅ SAME as CommunityView
                          const id = openId;
                          sessionStorage.setItem(
                            `mp:type:${openType}:${id}`,
                            openType
                          );
                          return navigate(`/marketplace/item/${id}`, {
                            state: { type: openType },
                          });
                        }}
                      />
                    );
                  })}
                </div>
              ) : (
                <div className="text-sm text-gray-500">
                  {canAct
                    ? lang === "ar"
                      ? "مفيش إعلانات عندك لسه."
                      : lang === "es"
                      ? "Aún no tienes anuncios."
                      : "You don't have any listings yet."
                    : lang === "ar"
                    ? "سجّل دخول علشان تشوف إعلاناتك."
                    : lang === "es"
                    ? "Inicia sesión para ver tus anuncios."
                    : "Login to see your listings."}
                </div>
              )}
            </div>
          ) : null}

          {!tabLoading && tab === "reviews" ? (
            <div className="space-y-3">
              {/* List */}
              {Array.isArray(reviews) && reviews.length ? (
                reviews.map((r, idx) => {
                  const stars = Number(r?.stars ?? r?.rating ?? 0) || 0;
                  const whoName = pickReviewName(r);
                  const whoId = pickReviewUserId(r);
                  const whoHref = whoId ? `/u/${whoId}` : "";

                  const itemTitle =
                    pickItemTitle(r) || (lang === "ar" ? "عنصر" : "Item");
                  const itemHref = pickItemHref(r);

                  const comment = String(
                    r?.comment ?? r?.commentText ?? r?.text ?? r?.body ?? ""
                  ).trim();

                  const when = fmtWhen(
                    r?.created_at || r?.createdAt || r?.date
                  );

                  return (
                    <div
                      key={`rv:${
                        r?.id ?? r?.review_id ?? r?.rating_id ?? "x"
                      }:${pickReviewUserId(r)}:${String(
                        r?.item_type || ""
                      )}:${Number(r?.item_id ?? r?.itemId ?? 0)}:${String(
                        r?.created_at || r?.createdAt || ""
                      )}:${idx}`}
                      className="bg-white border rounded-2xl p-4 md:p-5"
                    >
                      <div className="flex items-start gap-3">
                        {/* Avatar */}
                        <div className="shrink-0">
                          <div className="w-11 h-11 rounded-full bg-gray-100 border flex items-center justify-center font-bold text-gray-700">
                            {getInitials(pickReviewName(r))}
                          </div>
                        </div>

                        <div className="min-w-0 flex-1">
                          {/* Header: name + time */}
                          <div className="flex items-start justify-between gap-3">
                            <div className="min-w-0">
                              {(() => {
                                const whoId = pickReviewUserId(r);
                                const whoName = pickReviewName(r);
                                const whoHref = whoId ? `/u/${whoId}` : "";
                                return whoHref ? (
                                  <Link
                                    to={whoHref}
                                    className="font-semibold text-gray-900 hover:underline truncate"
                                    title={
                                      lang === "ar"
                                        ? "افتح الحساب"
                                        : "Open profile"
                                    }
                                  >
                                    {whoName}
                                  </Link>
                                ) : (
                                  <div className="font-semibold text-gray-900 truncate">
                                    {whoName}
                                  </div>
                                );
                              })()}

                              {(() => {
                                const when = fmtWhen(
                                  r?.created_at || r?.createdAt || r?.date
                                );
                                return when ? (
                                  <div className="text-xs text-gray-500 mt-1">
                                    {when}
                                  </div>
                                ) : null;
                              })()}
                            </div>

                            {/* Score */}
                            <div className="shrink-0 flex items-center gap-2">
                              <div className="px-2.5 py-1 rounded-xl bg-gray-50 border text-sm font-semibold text-gray-900">
                                {(
                                  Number(r?.stars ?? r?.rating ?? 0) || 0
                                ).toFixed(1)}
                                /5
                              </div>
                            </div>
                          </div>

                          {/* Stars row */}
                          <div className="mt-3 flex items-center justify-between gap-3">
                            <StarsRow
                              value={Number(r?.stars ?? r?.rating ?? 0) || 0}
                            />

                            {/* Item badge */}
                            {(() => {
                              const itemTitle =
                                pickItemTitle(r) ||
                                (lang === "ar" ? "عنصر" : "Item");
                              const itemHref = pickItemHref(r);
                              return itemHref ? (
                                <Link
                                  to={itemHref}
                                  className="max-w-[55%] truncate px-3 py-1.5 rounded-full bg-blue-50 text-blue-800 border border-blue-100 text-sm font-medium hover:underline"
                                  title={
                                    lang === "ar" ? "فتح الإعلان" : "Open item"
                                  }
                                >
                                  {lang === "ar"
                                    ? "تم التقييم على: "
                                    : "Reviewed: "}
                                  {itemTitle}
                                </Link>
                              ) : (
                                <div className="max-w-[55%] truncate px-3 py-1.5 rounded-full bg-gray-50 text-gray-700 border text-sm font-medium">
                                  {lang === "ar"
                                    ? "تم التقييم على: "
                                    : "Reviewed: "}
                                  {itemTitle}
                                </div>
                              );
                            })()}
                          </div>

                          {/* Comment bubble */}
                          {(() => {
                            const comment = String(
                              r?.comment ??
                                r?.commentText ??
                                r?.text ??
                                r?.body ??
                                ""
                            ).trim();

                            return comment ? (
                              <div className="mt-4 bg-gray-50 border rounded-2xl p-3 text-sm text-gray-800 leading-6 whitespace-pre-wrap">
                                {comment}
                              </div>
                            ) : (
                              <div className="mt-4 text-sm text-gray-400 italic">
                                {lang === "ar" ? "بدون تعليق" : "No comment"}
                              </div>
                            );
                          })()}

                          {/* Footer: action */}
                          {(() => {
                            const itemHref = pickItemHref(r);
                          })()}
                        </div>
                      </div>
                    </div>
                  );
                })
              ) : (
                <div className="text-sm text-gray-500">
                  {lang === "ar"
                    ? "لا توجد تقييمات بعد."
                    : lang === "es"
                    ? "Aún no hay reseñas."
                    : "No reviews yet."}
                </div>
              )}
            </div>
          ) : null}
        </div>
      </div>

      {/* Modals */}
      <Modal
        title={tt(lang, "editProfile")}
        open={editOpen}
        onClose={() => setEditOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setEditOpen(false)}
              className="px-4 py-2 rounded-xl border hover:bg-gray-50"
            >
              {tt(lang, "cancel")}
            </button>
            <button
              onClick={onSaveProfile}
              className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900"
            >
              {tt(lang, "saved")}
            </button>
          </div>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <Field
            label="Username"
            value={editForm.username}
            onChange={(v) => setEditForm((s) => ({ ...s, username: v }))}
            placeholder="example"
          />
          <Field
            label="Display name"
            value={editForm.display_name}
            onChange={(v) => setEditForm((s) => ({ ...s, display_name: v }))}
            placeholder="Your name"
          />
          <Field
            label="Avatar URL"
            value={editForm.avatar_url}
            onChange={(v) => setEditForm((s) => ({ ...s, avatar_url: v }))}
            placeholder="https://..."
          />
          <Field
            label="Cover URL"
            value={editForm.cover_url}
            onChange={(v) => setEditForm((s) => ({ ...s, cover_url: v }))}
            placeholder="https://..."
          />
          <Field
            label="Location"
            value={editForm.location}
            onChange={(v) => setEditForm((s) => ({ ...s, location: v }))}
            placeholder="Virginia, USA"
          />
          <Field
            label="Website"
            value={editForm.website}
            onChange={(v) => setEditForm((s) => ({ ...s, website: v }))}
            placeholder="google.com or https://..."
          />
          <Field
            label="Phone"
            value={editForm.phone}
            onChange={(v) => setEditForm((s) => ({ ...s, phone: v }))}
            placeholder="+1..."
          />
          <Field
            label="WhatsApp"
            value={editForm.whatsapp}
            onChange={(v) => setEditForm((s) => ({ ...s, whatsapp: v }))}
            placeholder="+20..."
          />
        </div>

        <div className="mt-3">
          <label className="text-sm font-medium">Bio</label>
          <textarea
            className="mt-1 w-full min-h-[110px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
            value={editForm.bio}
            onChange={(e) =>
              setEditForm((s) => ({ ...s, bio: e.target.value }))
            }
            placeholder="Tell people about you…"
          />
        </div>
      </Modal>

      <Modal
        title={tt(lang, "writeReview")}
        open={addReviewOpen}
        onClose={() => setAddReviewOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setAddReviewOpen(false)}
              className="px-4 py-2 rounded-xl border hover:bg-gray-50"
            >
              {tt(lang, "cancel")}
            </button>
            <button
              onClick={onCreateReview}
              className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900"
            >
              {tt(lang, "send")}
            </button>
          </div>
        }
      >
        <div className="space-y-3">
          <div>
            <label className="text-sm font-medium">Rating</label>
            <select
              className="mt-1 w-full border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={reviewForm.rating}
              onChange={(e) =>
                setReviewForm((s) => ({ ...s, rating: Number(e.target.value) }))
              }
            >
              {[5, 4, 3, 2, 1].map((n) => (
                <option key={n} value={n}>
                  {n} ⭐
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="text-sm font-medium">Comment</label>
            <textarea
              className="mt-1 w-full min-h-[120px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={reviewForm.comment}
              onChange={(e) =>
                setReviewForm((s) => ({ ...s, comment: e.target.value }))
              }
              placeholder="Your experience…"
            />
          </div>
        </div>
      </Modal>
    </div>
  );
}
