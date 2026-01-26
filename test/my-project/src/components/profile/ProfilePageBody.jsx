// src/components/ProfilePageBody.jsx
import React from "react";
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
  Briefcase,
  MessageCircle,
  Share2,
} from "lucide-react";

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
  ServicesTab,
  ProductsTab,
  ReviewsTab,
} from "./profilePage.parts";

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
  services,
  products,
  reviews,

  // counts
  countPosts,
  countServices,
  countProducts,
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
  onDeleteService,
  onDeleteProduct,

  onSaveProfile,
  onCreateService,
  onCreateProduct,
  onCreateReview,

  // ✅ NEW (avatar)
  onUploadAvatar,
  onDeleteAvatar,

  // modals state
  editOpen,
  setEditOpen,
  addServiceOpen,
  setAddServiceOpen,
  addProductOpen,
  setAddProductOpen,
  addReviewOpen,
  setAddReviewOpen,

  // forms
  editForm,
  setEditForm,
  serviceForm,
  setServiceForm,
  productForm,
  setProductForm,
  reviewForm,
  setReviewForm,

  // components (injected)
  PostCardComp,
  PostComposerComp,
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

  // ✅ cover src (supports relative + absolute + fallback to profile.cover_url)
  const coverSrcRaw = cover || profile.cover_url || "";
  const coverSrc = coverSrcRaw ? absUrl(API_BASE, coverSrcRaw) : "";

  return (
    <div className="max-w-5xl mx-auto p-4 md:p-6" dir={dir}>
      {/* ✅ Header Card (single cover only, facebook-style) */}
      <div className="relative rounded-2xl overflow-hidden border bg-white">
        {/* Cover */}
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

          {!coverSrc && canEdit ? (
            <label className="absolute inset-0 flex items-center justify-center cursor-pointer">
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
              <div className="px-4 py-2 rounded-xl bg-white/90 border text-sm text-gray-700 hover:bg-white shadow">
                {tt(lang, "add")} cover
              </div>
            </label>
          ) : null}
        </div>

        {/* Content */}
        <div className="p-4 md:p-6">
          <div className="flex flex-col md:flex-row md:items-end gap-4">
            <div className="-mt-12 md:-mt-16 flex items-end gap-4">
              {/* ===== Avatar ===== */}
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

                {/* ✅ avatar actions (edit/delete) */}
                {canAvatarActions ? (
                  <div className="absolute -bottom-2 -right-2 flex gap-2">
                    {/* Upload */}
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

                    {/* Delete */}
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

          {/* Bio + Meta */}
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
              services={countServices}
              products={countProducts}
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
            active={tab === "services"}
            onClick={() => setTab("services")}
            icon={<Briefcase size={16} />}
            label={`${tt(lang, "services")} (${countServices})`}
          />
          <TabPill
            active={tab === "products"}
            onClick={() => setTab("products")}
            icon={<Store size={16} />}
            label={`${tt(lang, "products")} (${countProducts})`}
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
            {canEdit && tab === "services" ? (
              <button
                onClick={() => setAddServiceOpen(true)}
                className="px-3 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
              >
                <Plus size={16} />
                {tt(lang, "addService")}
              </button>
            ) : null}

            {canEdit && tab === "products" ? (
              <button
                onClick={() => setAddProductOpen(true)}
                className="px-3 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2"
              >
                <Plus size={16} />
                {tt(lang, "addProduct")}
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

          {!tabLoading && tab === "services" ? (
            <ServicesTab
              lang={lang}
              items={services}
              isMe={canEdit}
              onDelete={onDeleteService}
            />
          ) : null}

          {!tabLoading && tab === "products" ? (
            <ProductsTab
              lang={lang}
              items={products}
              isMe={canEdit}
              onDelete={onDeleteProduct}
            />
          ) : null}

          {!tabLoading && tab === "reviews" ? (
            <ReviewsTab
              lang={lang}
              items={reviews}
              ratingAvg={ratingAvg}
              ratingCount={countReviews}
            />
          ) : null}
        </div>
      </div>

      {/* ===== Modals ===== */}
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
        {/* ✅ Avatar controls inside modal */}
        {canAvatarActions ? (
          <div className="mb-4 p-3 border rounded-2xl bg-gray-50 flex items-center justify-between gap-3">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-2xl overflow-hidden border bg-white">
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
                  <div className="w-full h-full flex items-center justify-center bg-gray-900 text-white font-extrabold">
                    {getInitials(displayName)}
                  </div>
                )}
              </div>
              <div>
                <div className="font-semibold text-sm">
                  {tt(lang, "avatar")}
                </div>
                <div className="text-xs text-gray-500">PNG/JPG up to ~2MB</div>
              </div>
            </div>

            <div className="flex gap-2">
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
                <div className="px-3 py-2 rounded-xl bg-black text-white hover:bg-gray-900 flex items-center gap-2">
                  <Pencil size={16} />
                  {tt(lang, "change")}
                </div>
              </label>

              <button
                type="button"
                onClick={() => onDeleteAvatar()}
                className="px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 text-red-600"
              >
                {tt(lang, "delete")}
              </button>
            </div>
          </div>
        ) : null}

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
        title={tt(lang, "addService")}
        open={addServiceOpen}
        onClose={() => setAddServiceOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setAddServiceOpen(false)}
              className="px-4 py-2 rounded-xl border hover:bg-gray-50"
            >
              {tt(lang, "cancel")}
            </button>
            <button
              onClick={onCreateService}
              className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900"
            >
              {tt(lang, "add")}
            </button>
          </div>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <Field
            label="Title"
            value={serviceForm.title}
            onChange={(v) => setServiceForm((s) => ({ ...s, title: v }))}
            placeholder="Electrician / Plumber…"
          />
          <Field
            label="Category"
            value={serviceForm.category}
            onChange={(v) => setServiceForm((s) => ({ ...s, category: v }))}
            placeholder="Home services…"
          />
          <div>
            <label className="text-sm font-medium">Price type</label>
            <select
              className="mt-1 w-full border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={serviceForm.price_type}
              onChange={(e) =>
                setServiceForm((s) => ({ ...s, price_type: e.target.value }))
              }
            >
              <option value="negotiable">Negotiable</option>
              <option value="fixed">Fixed</option>
              <option value="starting_at">Starting at</option>
            </select>
          </div>
          <Field
            label="Price value (optional)"
            value={serviceForm.price_value}
            onChange={(v) => setServiceForm((s) => ({ ...s, price_value: v }))}
            placeholder="e.g. 100"
          />
          <Field
            label="Location (optional)"
            value={serviceForm.location}
            onChange={(v) => setServiceForm((s) => ({ ...s, location: v }))}
            placeholder="Fairfax, VA"
          />
          <div className="md:col-span-2">
            <label className="text-sm font-medium">Description</label>
            <textarea
              className="mt-1 w-full min-h-[110px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={serviceForm.description}
              onChange={(e) =>
                setServiceForm((s) => ({ ...s, description: e.target.value }))
              }
              placeholder="Describe your service…"
            />
          </div>
        </div>
      </Modal>

      <Modal
        title={tt(lang, "addProduct")}
        open={addProductOpen}
        onClose={() => setAddProductOpen(false)}
        footer={
          <div className="flex justify-end gap-2">
            <button
              onClick={() => setAddProductOpen(false)}
              className="px-4 py-2 rounded-xl border hover:bg-gray-50"
            >
              {tt(lang, "cancel")}
            </button>
            <button
              onClick={onCreateProduct}
              className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900"
            >
              {tt(lang, "add")}
            </button>
          </div>
        }
      >
        <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
          <Field
            label="Title"
            value={productForm.title}
            onChange={(v) => setProductForm((s) => ({ ...s, title: v }))}
            placeholder="Item name…"
          />
          <Field
            label="Price (optional)"
            value={productForm.price}
            onChange={(v) => setProductForm((s) => ({ ...s, price: v }))}
            placeholder="e.g. 25"
          />
          <Field
            label="Currency"
            value={productForm.currency}
            onChange={(v) => setProductForm((s) => ({ ...s, currency: v }))}
            placeholder="USD"
          />
          <Field
            label="Location (optional)"
            value={productForm.location}
            onChange={(v) => setProductForm((s) => ({ ...s, location: v }))}
            placeholder="Alexandria, VA"
          />
          <div className="md:col-span-2">
            <label className="text-sm font-medium">Description</label>
            <textarea
              className="mt-1 w-full min-h-[110px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={productForm.description}
              onChange={(e) =>
                setProductForm((s) => ({ ...s, description: e.target.value }))
              }
              placeholder="Describe the product…"
            />
          </div>
          <div className="md:col-span-2">
            <label className="text-sm font-medium">
              Images URLs (one per line)
            </label>
            <textarea
              className="mt-1 w-full min-h-[110px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
              value={productForm.imagesText}
              onChange={(e) =>
                setProductForm((s) => ({ ...s, imagesText: e.target.value }))
              }
              placeholder={`https://...\nhttps://...`}
            />
          </div>
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
