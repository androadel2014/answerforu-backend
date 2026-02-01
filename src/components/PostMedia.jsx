import React, { useMemo } from "react";
import { getPostMediaUrls } from "../lib/postMedia";

export default function PostMedia({ post, className = "" }) {
  const urls = useMemo(() => getPostMediaUrls(post), [post]);

  if (!urls.length) return null;

  const many = urls.length > 1;

  return (
    <div className={["mt-2", className].filter(Boolean).join(" ")}>
      <div className={many ? "grid grid-cols-2 gap-2" : ""}>
        {urls.map((url, i) => (
          <img
            key={`${url}-${i}`}
            src={url}
            alt=""
            loading="lazy"
            className={[
              "w-full object-cover rounded-xl border border-gray-200",
              many ? "h-44" : "h-[360px]",
            ].join(" ")}
          />
        ))}
      </div>
    </div>
  );
}
