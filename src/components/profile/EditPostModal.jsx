// src/components/profile/EditPostModal.jsx
import React, { useEffect, useMemo, useState } from "react";
import toast from "react-hot-toast";
import { X, ImagePlus, Trash2 } from "lucide-react";

const cn = (...a) => a.filter(Boolean).join(" ");

const toArray = (v) => {
  if (!v) return [];
  if (Array.isArray(v)) return v.filter(Boolean);
  if (typeof v === "string") {
    const s = v.trim();
    if (!s) return [];
    if (s.startsWith("[")) {
      try {
        const arr = JSON.parse(s);
        return Array.isArray(arr) ? arr.filter(Boolean) : [];
      } catch {
        return [];
      }
    }
    return [s].filter(Boolean);
  }
  return [];
};

const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));

const absMediaUrl = (API_BASE, u) => {
  const s = String(u || "").trim();
  if (!s) return "";
  if (/^(data:|blob:|https?:\/\/)/i.test(s)) return s; // ✅ FIX
  return `${API_BASE}${s.startsWith("/") ? "" : "/"}${s}`;
};

export default function EditPostModal({
  open,
  onClose,
  lang,
  tt,
  API_BASE,
  post,
  Modal, // injected Modal from ProfilePage
  onSave, // async ({content, keepMedia, removeMedia, files, mediaPreview})
}) {
  const initialMedia = useMemo(() => {
    // prefer normalized post.media (array) but accept others
    const media = uniq([
      ...toArray(post?.media),
      ...toArray(post?.media_urls),
      ...toArray(post?.media_url),
      ...toArray(post?.image_url),
      ...toArray(post?.image),
      ...toArray(post?.images),
    ])
      .map((u) => absMediaUrl(API_BASE, u))
      .filter(Boolean);
    return media;
  }, [API_BASE, post]);

  const [content, setContent] = useState("");
  const [kept, setKept] = useState([]); // urls kept
  const [removed, setRemoved] = useState([]); // urls removed
  const [files, setFiles] = useState([]); // File[]
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!open) return;
    setContent(String(post?.content || ""));
    setKept(initialMedia);
    setRemoved([]);
    setFiles([]);
  }, [open, post, initialMedia]);

  const previews = useMemo(() => {
    const filePreviews = files.map((f) => ({
      key: `file_${f.name}_${f.size}_${f.lastModified}`,
      url: URL.createObjectURL(f),
      isFile: true,
    }));
    return [
      ...kept.map((u) => ({ key: `keep_${u}`, url: u, isFile: false })),
      ...filePreviews,
    ];
  }, [kept, files]);

  useEffect(() => {
    return () => {
      // revoke preview urls
      previews.forEach((p) => {
        if (p.isFile) {
          try {
            URL.revokeObjectURL(p.url);
          } catch {}
        }
      });
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const removeKeptUrl = (url) => {
    setKept((xs) => xs.filter((x) => x !== url));
    setRemoved((xs) => uniq([...xs, url]));
  };

  const removeFileAt = (idx) => {
    setFiles((xs) => xs.filter((_, i) => i !== idx));
  };

  async function handleSave() {
    const c = String(content || "").trim();
    if (!c) return toast.error(tt(lang, "postContentRequired"));

    const mediaPreview = uniq([
      ...kept,
      ...files.map((f) => URL.createObjectURL(f)).filter(Boolean), // just for optimistic preview
    ]);

    setSaving(true);
    try {
      await onSave?.({
        content: c,
        keepMedia: kept,
        removeMedia: removed,
        files,
        mediaPreview,
      });
      onClose?.();
    } catch (e) {
      toast.error(e?.message || tt(lang, "updateFailed"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Modal
      title={tt(lang, "edit")}
      open={open}
      onClose={() => (saving ? null : onClose?.())}
      footer={
        <div className="flex items-center justify-end gap-2">
          <button
            onClick={() => onClose?.()}
            disabled={saving}
            className="px-4 py-2 rounded-xl border hover:bg-gray-50 disabled:opacity-50"
          >
            {tt(lang, "cancel")}
          </button>
          <button
            onClick={handleSave}
            disabled={saving}
            className="px-4 py-2 rounded-xl bg-black text-white hover:bg-gray-900 disabled:opacity-50"
          >
            {saving ? tt(lang, "saving") || "Saving..." : tt(lang, "saved")}
          </button>
        </div>
      }
    >
      <div className="space-y-4">
        <div>
          <label className="text-sm font-medium">Content</label>
          <textarea
            className="mt-2 w-full min-h-[160px] border rounded-2xl p-3 outline-none focus:ring-2 focus:ring-black/10"
            value={content}
            onChange={(e) => setContent(e.target.value)}
            placeholder="Update your post…"
          />
        </div>

        <div className="flex items-center justify-between gap-3">
          <div className="text-sm font-semibold">
            {tt(lang, "photos") || "Photos"}
          </div>

          <label className="inline-flex items-center gap-2 px-3 py-2 rounded-xl border bg-white hover:bg-gray-50 cursor-pointer">
            <ImagePlus size={16} />
            <span className="text-sm font-semibold">
              {tt(lang, "addPhotos") || "Add photos"}
            </span>
            <input
              type="file"
              className="hidden"
              multiple
              accept="image/*"
              onChange={(e) => {
                const list = Array.from(e.target.files || []);
                if (!list.length) return;
                setFiles((xs) => [...xs, ...list]);
                e.target.value = "";
              }}
            />
          </label>
        </div>

        {previews.length ? (
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {kept.map((url) => (
              <div
                key={`k_${url}`}
                className="relative rounded-2xl overflow-hidden border bg-gray-50"
              >
                <img src={url} alt="" className="w-full h-32 object-cover" />
                <button
                  type="button"
                  onClick={() => removeKeptUrl(url)}
                  className="absolute bottom-2 left-2 px-2 py-1 rounded-xl bg-white/90 border text-xs font-semibold hover:bg-white inline-flex items-center gap-1"
                >
                  <Trash2 size={14} />
                  {tt(lang, "remove") || "Remove"}
                </button>
              </div>
            ))}

            {files.map((f, idx) => {
              const url = URL.createObjectURL(f);
              return (
                <div
                  key={`f_${f.name}_${f.size}_${f.lastModified}_${idx}`}
                  className="relative rounded-2xl overflow-hidden border bg-gray-50"
                >
                  <img src={url} alt="" className="w-full h-32 object-cover" />
                  <button
                    type="button"
                    onClick={() => removeFileAt(idx)}
                    className="absolute bottom-2 left-2 px-2 py-1 rounded-xl bg-white/90 border text-xs font-semibold hover:bg-white inline-flex items-center gap-1"
                  >
                    <Trash2 size={14} />
                    {tt(lang, "remove") || "Remove"}
                  </button>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="text-sm text-gray-500">
            {tt(lang, "noPhotos") || "No photos"}
          </div>
        )}
      </div>
    </Modal>
  );
}
