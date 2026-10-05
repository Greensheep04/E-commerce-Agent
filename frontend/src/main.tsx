import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import CustomerApp from "./CustomerApp";
import AssessmentWorkspace from "./assessment/AssessmentWorkspace";
import DataCenterWorkspace from "./data-center/DataCenterWorkspace";
import "./customer.css";
import "./styles.css";

const customerWorkspace =
  window.location.pathname.endsWith("/customer.html") ||
  new URLSearchParams(window.location.search).get("workspace") === "customer";
const assessmentWorkspace =
  new URLSearchParams(window.location.search).get("workspace") === "assessment";
const dataCenterWorkspace =
  new URLSearchParams(window.location.search).get("workspace") === "data-center";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    {dataCenterWorkspace ? <DataCenterWorkspace /> : assessmentWorkspace ? <AssessmentWorkspace /> : customerWorkspace ? <CustomerApp /> : <App />}
  </React.StrictMode>,
);
