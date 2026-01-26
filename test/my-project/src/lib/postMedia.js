export const getPostMediaUrls = (p) => {
  const v = p?.media_urls ?? p?.mediaUrls ?? p?.images ?? p?.media;
  if (!v) return [];
  if (Array.isArray(v)) return v.filter(Boolean);
  if (typeof v === "string") {
    try {
      const arr = JSON.parse(v);
      return Array.isArray(arr) ? arr.filter(Boolean) : [];
    } catch {
      // لو string واحدة (URL)
      return v.trim() ? [v.trim()] : [];
    }
  }
  return [];
};

export const postHasImages = (p) => getPostMediaUrls(p).length > 0;
