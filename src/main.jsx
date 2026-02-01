import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import App from "./App.jsx";

import "./i18n/index"; // ✅ correct (relative to src)

createRoot(document.getElementById("root")).render(<App />);