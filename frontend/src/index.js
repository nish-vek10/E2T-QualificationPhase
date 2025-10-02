// src/index.js
(function () {
  try {
    const qp = new URLSearchParams(window.location.search);
    if (qp.get('viewport') === 'mobile') {
      document.documentElement.classList.add('force-mobile');
    }
  } catch (e) {}
})();

import React from "react";
import { createRoot } from "react-dom/client";
import "./index.css";
import "./responsive.css";
import App from "./App";
import CountryAllocation from "./CountryAllocation";

const container = document.getElementById("root");
const root = createRoot(container);

// choose view via ?view=alloc
const qp = new URLSearchParams(window.location.search);
const view = qp.get("view");

root.render(view === "alloc" ? <CountryAllocation /> : <App />);
