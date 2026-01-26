// src/components/ProfilePage.jsx  (FULL FILE - copy/paste)
// ✅ split into:
//    - src/components/ProfilePageBody.jsx
//    - src/components/profilePage.parts.jsx
// ✅ نفس المنطق + نفس ال UI (بس organized)
import PostCard from "./profile/PostCard";
import React, { useEffect, useMemo, useState } from "react";
import { useParams, useNavigate } from "react-router-dom";
import toast from "react-hot-toast";
import {
  MapPin,
  Link as LinkIcon,
  Phone,
  BadgeCheck,
  Plus,
  Pencil,
  X,
  Star,
  Store,
  Briefcase,
  MessageCircle,
  Share2,
  Trash2,
  SendHorizontal,
  ChevronDown,
  ChevronUp,
  ThumbsUp,
  Users,
  UserPlus,
} from "lucide-react";
import PostComposer from "../components/feed/PostComposer";

import ProfilePageBody from "./profile/ProfilePageBody";

import {
  tt,
  getDir,
  getAPIBase,
  authHeaders,
  isAuthed,
  classNames,
  getInitials,
  safeUrl,
  extractNumericId,
  getAuthUserId,
  getPostId,
  normId,
  formatTime,
  getCategory,
  toastConfirm,
  tryFetch,
  tryFetchFallback,
  Modal,
  absUrl,
  toArr,
  uniq,
  buildUpdateFormData,
  normalizePostForMedia,
  normalizeFeedPostId,
  CommentNode,
  buildCommentTree,
} from "./profile/profilePage.parts";

/* =========================
   Main Page
========================= */
export function ProfilePage({ lang = "en" }) {
  const API_BASE = useMemo(() => getAPIBase(), []);
  const { userId } = useParams();
  const navigate = useNavigate();
  const dir = getDir(lang);

  const [loading, setLoading] = useState(true);

  const [profile, setProfile] = useState(null);
  const [stats, setStats] = useState(null);
  const [isMe, setIsMe] = useState(false);
  const [isFollowing, setIsFollowing] = useState(false);

  const [tab, setTab] = useState("posts"); // posts | services | products | reviews

  const [posts, setPosts] = useState([]);
  const [services, setServices] = useState([]);
  const [products, setProducts] = useState([]);

  const [reviews, setReviews] = useState([]);

  const [tabLoading, setTabLoading] = useState(false);

  const [editOpen, setEditOpen] = useState(false);
  const [addServiceOpen, setAddServiceOpen] = useState(false);
  const [addProductOpen, setAddProductOpen] = useState(false);
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

  const [serviceForm, setServiceForm] = useState({
    title: "",
    description: "",
    category: "",
    price_type: "negotiable",
    price_value: "",
    location: "",
  });

  const [productForm, setProductForm] = useState({
    title: "",
    description: "",
    price: "",
    currency: "USD",
    location: "",
    imagesText: "",
  });

  const [reviewForm, setReviewForm] = useState({ rating: 5, comment: "" });

  const canAct = isAuthed();
  const authedId = getAuthUserId();
  const computedIsMe =
    !!authedId && !!userId && normId(authedId) === normId(String(userId));
  const canEdit = canAct && (isMe || computedIsMe);

  const countPosts = Number(stats?.posts ?? posts.length ?? 0) || 0;
  const countServices = Number(stats?.services ?? services.length ?? 0) || 0;
  const countProducts = Number(stats?.products ?? products.length ?? 0) || 0;
  const countReviews = Number(stats?.ratingCount ?? reviews.length ?? 0) || 0;

  useEffect(() => {
    let dead = false;

    async function load() {
      setLoading(true);
      try {
        const uid = String(userId || "").trim();
        if (!uid) throw new Error("Missing userId");

        const data = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}`,
            `${API_BASE}/api/profiles/${uid}`,
            `${API_BASE}/api/user/${uid}/profile`,
            `${API_BASE}/api/users/${uid}/profile`,
            `${API_BASE}/api/users/${uid}`,
          ],
          { headers: { ...authHeaders() } }
        );

        if (dead) return;

        const p =
          data?.profile || data?.user_profile || data?.user || data || null;
        const st = data?.stats || data?.profile_stats || null;

        setProfile(p);
        setStats(st);

        const meId = getAuthUserId();
        const fallbackIsMe = !!meId && normId(meId) === normId(uid);

        setIsMe(!!data?.isMe || fallbackIsMe);
        setIsFollowing(
          typeof data?.isFollowing === "boolean" ? data.isFollowing : false
        );

        setEditForm({
          username: p?.username || "",
          display_name: p?.display_name || "",
          avatar_url: p?.avatar_url || "",
          cover_url: p?.cover_url || "",
          bio: p?.bio || "",
          location: p?.location || "",
          phone: p?.phone || "",
          whatsapp: p?.whatsapp || "",
          website: p?.website || "",
        });
      } catch (e) {
        toast.error(e.message || tt(lang, "failedLoadProfile"));
        setProfile(null);
        setStats(null);
      } finally {
        if (!dead) setLoading(false);
      }
    }

    load();
    return () => {
      dead = true;
    };
  }, [API_BASE, userId, lang]);

  useEffect(() => {
    if (!userId) return;
    let dead = false;

    async function loadTab() {
      setTabLoading(true);
      try {
        const uid = String(userId || "").trim();
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
            { headers: { ...authHeaders() } }
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

        if (tab === "services") {
          const r = await tryFetchFallback(
            [
              `${API_BASE}/api/profile/${uid}/services`,
              `${API_BASE}/api/services/user/${uid}`,
              `${API_BASE}/api/users/${uid}/services`,
            ],
            { headers: { ...authHeaders() } }
          );
          const items =
            r?.services || r?.items || r?.data || (Array.isArray(r) ? r : []);
          if (!dead) setServices(Array.isArray(items) ? items : []);
        }

        if (tab === "products") {
          const r = await tryFetchFallback(
            [
              `${API_BASE}/api/profile/${uid}/products`,
              `${API_BASE}/api/products/user/${uid}`,
              `${API_BASE}/api/users/${uid}/products`,
            ],
            { headers: { ...authHeaders() } }
          );
          const items =
            r?.products || r?.items || r?.data || (Array.isArray(r) ? r : []);
          if (!dead) setProducts(Array.isArray(items) ? items : []);
        }

        if (tab === "reviews") {
          const r = await tryFetchFallback(
            [
              `${API_BASE}/api/profile/${uid}/reviews`,
              `${API_BASE}/api/reviews/user/${uid}`,
              `${API_BASE}/api/users/${uid}/reviews`,
            ],
            { headers: { ...authHeaders() } }
          );
          const items =
            r?.reviews || r?.items || r?.data || (Array.isArray(r) ? r : []);
          if (!dead) setReviews(Array.isArray(items) ? items : []);
        }
      } catch (e) {
        toast.error(e.message || tt(lang, "failedLoadTab"));
        if (!dead) {
          if (tab === "posts") setPosts([]);
          if (tab === "services") setServices([]);
          if (tab === "products") setProducts([]);
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
  }, [API_BASE, userId, tab, lang]);

  const cover = absUrl(API_BASE, profile?.cover_url || "");
  const avatar = absUrl(API_BASE, profile?.avatar_url || "");

  const displayName = profile?.display_name || profile?.username || "User";
  const username = profile?.username ? `@${profile.username}` : "";
  const verified = !!profile?.is_verified;

  /* =========================
     ✅ Avatar Upload/Delete
  ========================= */

  /* =========================
     ✅ Cover Upload/Delete
     (needs backend routes like avatar)
  ========================= */
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
        {
          method: "POST",
          headers: { ...authHeaders() }, // ✅ بدون Content-Type
          body: fd,
        }
      );

      const nextUrl =
        r?.cover_url ||
        r?.url ||
        r?.profile?.cover_url ||
        r?.user_profile?.cover_url ||
        "";

      setProfile((p) => ({ ...(p || {}), cover_url: nextUrl }));
      setEditForm((f) => ({ ...f, cover_url: nextUrl }));
      toast.success(tt(lang, "saved"));
    } catch (e) {
      // غالبًا 404 = routes مش موجودة في الباك
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
        { method: "DELETE", headers: { ...authHeaders() } }
      );

      setProfile((p) => ({ ...(p || {}), cover_url: null }));
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
        {
          method: "POST",
          headers: { ...authHeaders() }, // ✅ بدون Content-Type (FormData)
          body: fd,
        }
      );

      const nextUrl =
        r?.avatar_url ||
        r?.url ||
        r?.profile?.avatar_url ||
        r?.user_profile?.avatar_url ||
        "";

      setProfile((p) => ({ ...(p || {}), avatar_url: nextUrl }));
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
        { method: "DELETE", headers: { ...authHeaders() } }
      );

      setProfile((p) => ({ ...(p || {}), avatar_url: null }));
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
            `${API_BASE}/api/profile/${userId}/follow`,
            `${API_BASE}/api/follow/${userId}`,
          ],
          {
            method: "DELETE",
            headers: { "Content-Type": "application/json", ...authHeaders() },
          }
        );
        setIsFollowing(false);
        setStats((s) =>
          s ? { ...s, followers: Math.max(0, (s.followers || 0) - 1) } : s
        );
      } else {
        await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${userId}/follow`,
            `${API_BASE}/api/follow/${userId}`,
          ],
          {
            method: "POST",
            headers: { "Content-Type": "application/json", ...authHeaders() },
          }
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
    try {
      const payload = { ...editForm };
      const r = await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me`,
          `${API_BASE}/api/me/profile`,
          `${API_BASE}/api/user/profile/me`,
        ],
        {
          method: "PUT",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify(payload),
        }
      );
      toast.success(tt(lang, "saved"));
      setProfile(r.profile || r.user_profile || r.user || r);
      setEditOpen(false);
    } catch (e) {
      toast.error(e.message || tt(lang, "saveFailed"));
    }
  }

  async function refreshCurrentTab() {
    try {
      const uid = String(userId || "").trim();
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
          { headers: { ...authHeaders() } }
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

      if (tab === "services") {
        const r = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}/services`,
            `${API_BASE}/api/services/user/${uid}`,
            `${API_BASE}/api/users/${uid}/services`,
          ],
          { headers: { ...authHeaders() } }
        );
        const items =
          r?.services || r?.items || r?.data || (Array.isArray(r) ? r : []);
        setServices(Array.isArray(items) ? items : []);
      }

      if (tab === "products") {
        const r = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}/products`,
            `${API_BASE}/api/products/user/${uid}`,
            `${API_BASE}/api/users/${uid}/products`,
          ],
          { headers: { ...authHeaders() } }
        );
        const items =
          r?.products || r?.items || r?.data || (Array.isArray(r) ? r : []);
        setProducts(Array.isArray(items) ? items : []);
      }

      if (tab === "reviews") {
        const r = await tryFetchFallback(
          [
            `${API_BASE}/api/profile/${uid}/reviews`,
            `${API_BASE}/api/reviews/user/${uid}`,
            `${API_BASE}/api/users/${uid}/reviews`,
          ],
          { headers: { ...authHeaders() } }
        );
        const items =
          r?.reviews || r?.items || r?.data || (Array.isArray(r) ? r : []);
        setReviews(Array.isArray(items) ? items : []);
      }
    } catch {}
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
        (p) => normId(getPostId(p)) !== target
      )
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
        { method: "DELETE", headers: { ...authHeaders() } }
      );

      toast.success(tt(lang, "deleted"));
      setStats((s) =>
        s ? { ...s, posts: Math.max(0, (s.posts || 0) - 1) } : s
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
      payload?.content ?? payload?.text ?? payload?.body ?? ""
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
      })
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

  async function onCreateService() {
    if (!canEdit) return;
    const title = String(serviceForm.title || "").trim();
    if (!title) return toast.error("Title is required");

    const priceValue =
      serviceForm.price_value === "" ? null : Number(serviceForm.price_value);

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/services`,
          `${API_BASE}/api/me/profile/services`,
          `${API_BASE}/api/services/me`,
        ],
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({
            ...serviceForm,
            title,
            price_value: Number.isFinite(priceValue) ? priceValue : null,
          }),
        }
      );
      toast.success(tt(lang, "serviceAdded"));
      setAddServiceOpen(false);
      setServiceForm({
        title: "",
        description: "",
        category: "",
        price_type: "negotiable",
        price_value: "",
        location: "",
      });
      setTab("services");
      await refreshCurrentTab();
      setStats((s) => (s ? { ...s, services: (s.services || 0) + 1 } : s));
    } catch (e) {
      toast.error(e.message || tt(lang, "createServiceFailed"));
    }
  }

  async function onDeleteService(id) {
    if (!canEdit) return;

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteServiceQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    const prev = services;
    setServices((xs) =>
      (Array.isArray(xs) ? xs : []).filter((s) => (s.id ?? s.service_id) !== id)
    );

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/services/${id}`,
          `${API_BASE}/api/me/profile/services/${id}`,
          `${API_BASE}/api/services/me/${id}`,
        ],
        { method: "DELETE", headers: { ...authHeaders() } }
      );
      toast.success(tt(lang, "deleted"));
      setStats((st) =>
        st ? { ...st, services: Math.max(0, (st.services || 0) - 1) } : st
      );
      await refreshCurrentTab();
    } catch (e) {
      setServices(prev);
      toast.error(e.message || tt(lang, "deleteFailed"));
    }
  }

  async function onCreateProduct() {
    if (!canEdit) return;
    const title = String(productForm.title || "").trim();
    if (!title) return toast.error("Title is required");

    const price = productForm.price === "" ? null : Number(productForm.price);
    const images = String(productForm.imagesText || "")
      .split("\n")
      .map((x) => x.trim())
      .filter(Boolean);

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/products`,
          `${API_BASE}/api/me/profile/products`,
          `${API_BASE}/api/products/me`,
        ],
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({
            title,
            description: String(productForm.description || "").trim() || null,
            price: Number.isFinite(price) ? price : null,
            currency: String(productForm.currency || "USD").trim() || "USD",
            location: String(productForm.location || "").trim() || null,
            images,
          }),
        }
      );
      toast.success(tt(lang, "productAdded"));
      setAddProductOpen(false);
      setProductForm({
        title: "",
        description: "",
        price: "",
        currency: "USD",
        location: "",
        imagesText: "",
      });
      setTab("products");
      await refreshCurrentTab();
      setStats((s) => (s ? { ...s, products: (s.products || 0) + 1 } : s));
    } catch (e) {
      toast.error(e.message || tt(lang, "createProductFailed"));
    }
  }

  async function onDeleteProduct(id) {
    if (!canEdit) return;

    const ok = await toastConfirm({
      lang,
      title: tt(lang, "deleteProductQ"),
      confirmText: tt(lang, "confirmDelete"),
    });
    if (!ok) return;

    const prev = products;
    setProducts((xs) =>
      (Array.isArray(xs) ? xs : []).filter((p) => (p.id ?? p.product_id) !== id)
    );

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/me/products/${id}`,
          `${API_BASE}/api/me/profile/products/${id}`,
          `${API_BASE}/api/products/me/${id}`,
        ],
        { method: "DELETE", headers: { ...authHeaders() } }
      );
      toast.success(tt(lang, "deleted"));
      setStats((st) =>
        st ? { ...st, products: Math.max(0, (st.products || 0) - 1) } : st
      );
      await refreshCurrentTab();
    } catch (e) {
      setProducts(prev);
      toast.error(e.message || tt(lang, "deleteFailed"));
    }
  }

  async function onCreateReview() {
    if (!canAct) return toast.error(tt(lang, "loginFirst"));
    if (isMe || computedIsMe) return toast.error(tt(lang, "cannotReviewSelf"));

    const rating = Number(reviewForm.rating);
    const comment = String(reviewForm.comment || "").trim();
    if (!comment) return toast.error(tt(lang, "writeYourComment"));
    if (!Number.isFinite(rating) || rating < 1 || rating > 5)
      return toast.error(tt(lang, "ratingBad"));

    try {
      await tryFetchFallback(
        [
          `${API_BASE}/api/profile/${userId}/reviews`,
          `${API_BASE}/api/reviews/${userId}`,
          `${API_BASE}/api/users/${userId}/reviews`,
        ],
        {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ rating, comment }),
        }
      );
      toast.success(tt(lang, "reviewSent"));
      setAddReviewOpen(false);
      setReviewForm({ rating: 5, comment: "" });
      setTab("reviews");
      await refreshCurrentTab();
    } catch (e) {
      toast.error(e.message || tt(lang, "reviewFailed"));
    }
  }

  async function onShare() {
    const url = window.location.href;
    const text = `Profile on AnswerForU: ${displayName}`;

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
  }

  const ratingAvg = Number(stats?.ratingAvg ?? 0) || 0;
  const followers = Number(stats?.followers ?? 0) || 0;
  const following = Number(stats?.following ?? 0) || 0;

  return (
    <ProfilePageBody
      lang={lang}
      dir={dir}
      API_BASE={API_BASE}
      navigate={navigate}
      loading={loading}
      profile={profile}
      stats={stats}
      isFollowing={isFollowing}
      canEdit={canEdit}
      canAct={canAct}
      tab={tab}
      setTab={setTab}
      tabLoading={tabLoading}
      posts={posts}
      services={services}
      products={products}
      reviews={reviews}
      countPosts={countPosts}
      countServices={countServices}
      countProducts={countProducts}
      countReviews={countReviews}
      ratingAvg={ratingAvg}
      followers={followers}
      following={following}
      cover={cover}
      avatar={avatar}
      displayName={displayName}
      username={username}
      verified={verified}
      onShare={onShare}
      onFollowToggle={onFollowToggle}
      onDeletePost={onDeletePost}
      onUpdatePost={onUpdatePost}
      refreshCurrentTab={refreshCurrentTab}
      onDeleteService={onDeleteService}
      onDeleteProduct={onDeleteProduct}
      onSaveProfile={onSaveProfile}
      onCreateService={onCreateService}
      onCreateProduct={onCreateProduct}
      onCreateReview={onCreateReview}
      editOpen={editOpen}
      setEditOpen={setEditOpen}
      addServiceOpen={addServiceOpen}
      setAddServiceOpen={setAddServiceOpen}
      addProductOpen={addProductOpen}
      setAddProductOpen={setAddProductOpen}
      addReviewOpen={addReviewOpen}
      setAddReviewOpen={setAddReviewOpen}
      editForm={editForm}
      setEditForm={setEditForm}
      serviceForm={serviceForm}
      setServiceForm={setServiceForm}
      productForm={productForm}
      setProductForm={setProductForm}
      reviewForm={reviewForm}
      setReviewForm={setReviewForm}
      PostCardComp={PostCard}
      PostComposerComp={PostComposer}
      /* ✅ NEW */
      onUploadAvatar={onUploadAvatar}
      onDeleteAvatar={onDeleteAvatar}
      onUploadCover={onUploadCover}
      onDeleteCover={onDeleteCover}
    />
  );
}

/* ✅ default export */
export default ProfilePage;
