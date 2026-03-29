const API_CONFIG = {
    train: {
        inputId: "train-file",
        fileNameId: "train-file-name",
        previewId: "uploaded-train-preview",
        statusId: "train-upload-status",
        uploadUrl: "/train/upload",
        uploadLabel: "training",
        stageStatusId: "stage-train-status",
    },
    score: {
        inputId: "score-file",
        fileNameId: "score-file-name",
        previewId: "uploaded-scoring-preview",
        statusId: "score-upload-status",
        uploadUrl: "/score/upload",
        uploadLabel: "scoring",
        stageStatusId: "stage-score-status",
    },
};

const UI_STATE = {
    trainingRows: [],
    scoringRows: [],
    scoredRows: [],
    resultsRows: [],
};

document.addEventListener("DOMContentLoaded", () => {
    initializeUi();
});

function initializeUi() {
    showSection("train-section");
    initializeFileInputs();
    initializeResultsFilters();
    renderEmailPlaceholderState(
        "Run inference first, then preview how the future email lane will be staged."
    );
    showAppNotice(
        "Training, scoring, and result browsing are live. Email actions remain placeholder-only until the backend is rebuilt.",
        "warning"
    );
}

function initializeFileInputs() {
    Object.values(API_CONFIG).forEach((config) => {
        const input = document.getElementById(config.inputId);
        if (!input) {
            return;
        }

        input.addEventListener("change", () => {
            const selectedName = input.files?.[0]?.name;
            const hint = document.getElementById(config.fileNameId);
            if (!hint) {
                return;
            }

            hint.textContent = selectedName
                ? `Selected file: ${selectedName}`
                : `No ${config.uploadLabel} file selected yet.`;
        });
    });
}

function initializeResultsFilters() {
    ["table-type", "version-type"].forEach((id) => {
        const element = document.getElementById(id);
        if (!element) {
            return;
        }
        element.addEventListener("change", viewResults);
    });
}

function showSection(sectionId) {
    document.querySelectorAll(".section").forEach((section) => {
        section.classList.add("hidden");
    });

    document.getElementById(sectionId)?.classList.remove("hidden");

    document.querySelectorAll(".nav-button").forEach((button) => {
        const isActive = button.dataset.sectionTarget === sectionId;
        button.classList.toggle("is-active", isActive);
    });
}

function normalizeTableData(data) {
    if (Array.isArray(data)) {
        return data;
    }
    if (data && typeof data === "object") {
        return [data];
    }
    return [];
}

function toNumber(id, fallbackValue) {
    const rawValue = document.getElementById(id)?.value;
    const parsedValue = Number(rawValue);
    return Number.isFinite(parsedValue) ? parsedValue : fallbackValue;
}

function formatDateTime(dateValue) {
    if (!dateValue) {
        return "";
    }

    const parsedDate = new Date(dateValue);
    if (Number.isNaN(parsedDate.getTime())) {
        return String(dateValue);
    }

    return parsedDate.toLocaleString();
}

function buildMetadataLine(timeTaken, dateCreated) {
    const fragments = [];

    if (timeTaken !== undefined && timeTaken !== null && timeTaken !== "") {
        fragments.push(`Runtime ${Number(timeTaken).toFixed(2)} seconds`);
    }
    if (dateCreated) {
        fragments.push(`Logged ${formatDateTime(dateCreated)}`);
    }

    return fragments.join(" | ");
}

function showAppNotice(message, tone = "neutral") {
    const notice = document.getElementById("app-notice");
    if (!notice) {
        return;
    }

    notice.textContent = message;
    notice.className = `notice-bar notice-${tone}`;
}

function setStatusMessage(id, message, tone = "neutral") {
    const element = document.getElementById(id);
    if (!element) {
        return;
    }

    element.textContent = message;
    element.className = `inline-status tone-${tone}`;
}

function setStageStatus(id, message, tone = "neutral") {
    const element = document.getElementById(id);
    if (!element) {
        return;
    }

    element.textContent = message;
    element.className = `tone-${tone}`;
}

function setButtonEnabled(buttonId, enabled) {
    const button = document.getElementById(buttonId);
    if (!button) {
        return;
    }

    button.disabled = !enabled;
    button.classList.toggle("is-disabled", !enabled);

    if (enabled) {
        button.removeAttribute("title");
    } else if (button.dataset.disabledTitle) {
        button.title = button.dataset.disabledTitle;
    }
}

function startProgress(spinnerId, containerId, barId, labelId) {
    const spinner = document.getElementById(spinnerId);
    const container = document.getElementById(containerId);
    const bar = document.getElementById(barId);
    const label = document.getElementById(labelId);

    if (!spinner || !container || !bar || !label) {
        return null;
    }

    spinner.style.display = "block";
    container.classList.remove("hidden-progress");
    bar.style.width = "0%";
    label.textContent = "0%";

    let percent = 0;
    const totalDuration = 900000;
    const interval = 1000;
    const increment = 100 / (totalDuration / interval);

    const timer = setInterval(() => {
        percent = Math.min(100, percent + increment);
        bar.style.width = `${percent}%`;
        label.textContent = `${Math.floor(percent)}%`;
    }, interval);

    return { spinner, container, bar, label, timer };
}

function stopProgress(progressState, markComplete = false) {
    if (!progressState) {
        return;
    }

    clearInterval(progressState.timer);
    progressState.bar.style.width = markComplete ? "100%" : "0%";
    progressState.label.textContent = markComplete ? "100%" : "0%";
    progressState.spinner.style.display = "none";
    progressState.container.classList.add("hidden-progress");
}

async function fetchJson(url, options = {}, fallbackError = "Request failed.") {
    const response = await fetch(url, options);
    let data = null;

    try {
        data = await response.json();
    } catch (error) {
        data = null;
    }

    if (!response.ok) {
        const detail = Array.isArray(data?.detail)
            ? data.detail.map((item) => item.msg).join(", ")
            : data?.detail;
        const statusText = data?.dicStatus ? JSON.stringify(data.dicStatus) : "";
        throw new Error(detail || statusText || fallbackError);
    }

    return data;
}

async function uploadCSV(type) {
    const config = API_CONFIG[type];
    const fileInput = document.getElementById(config?.inputId);
    const file = fileInput?.files?.[0];

    if (!config) {
        showAppNotice("Unknown upload type.", "danger");
        return;
    }

    if (!file) {
        setStatusMessage(config.statusId, "Select a CSV file first.", "danger");
        showAppNotice(`Select a ${config.uploadLabel} CSV before uploading.`, "danger");
        return;
    }

    const formData = new FormData();
    formData.append("objFile", file);

    setStatusMessage(config.statusId, `Uploading ${config.uploadLabel} dataset...`, "neutral");
    showAppNotice(`Uploading ${config.uploadLabel} dataset...`, "neutral");

    try {
        const data = await fetchJson(
            config.uploadUrl,
            {
                method: "POST",
                body: formData,
            },
            `Upload failed for ${config.uploadLabel}.`
        );

        const rows = normalizeTableData(data.tblOutput);
        if (type === "train") {
            UI_STATE.trainingRows = rows;
        } else if (type === "score") {
            UI_STATE.scoringRows = rows;
        }

        displayTable(rows, config.previewId, "No data to display.");
        setStatusMessage(
            config.statusId,
            `${rows.length} ${config.uploadLabel} row(s) loaded into the workspace.`,
            "success"
        );
        setStageStatus(config.stageStatusId, "Dataset ready", "success");
        showAppNotice(
            `${capitalize(config.uploadLabel)} dataset uploaded successfully.`,
            "success"
        );
    } catch (error) {
        console.error("Upload error:", error);
        setStatusMessage(
            config.statusId,
            error.message || `Upload failed for ${config.uploadLabel}.`,
            "danger"
        );
        showAppNotice(
            error.message || `Upload failed for ${config.uploadLabel}.`,
            "danger"
        );
    }
}

function getTrainingRequestBody() {
    return {
        intRandomState: toNumber("train-random-state", 0),
        intTopFeats: toNumber("train-top-feats", 20),
        fltF1: toNumber("train-f1-threshold", 1),
    };
}

async function trainModel() {
    const progress = startProgress(
        "loading-spinner-training-details-preview",
        "progress-bar-container-train",
        "progress-bar-train",
        "progress-label-train"
    );
    const timeDetails = document.getElementById("time-details");
    const featureImportanceSection = document.getElementById("feature-importance-section");
    const featureImportancePreview = document.getElementById("feature-importance-preview");

    showAppNotice("Training model with the current backend route...", "neutral");

    try {
        const data = await fetchJson(
            "/train/model",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(getTrainingRequestBody()),
            },
            "Model training failed."
        );

        stopProgress(progress, true);

        renderKeyValueCards(data.objMetrics, "training-metric-cards");
        renderKeyValueCards(data.objDatasetSplit, "training-split-cards");
        renderKeyValueCards(data.objConfusionMatrix, "training-confusion-cards");

        displayTable(data.objDatasetSplit, "training-details-preview", "No dataset split available.");
        displayTable(data.objMetrics, "metrics-details-preview", "No model metrics available.");
        displayTable(
            data.objConfusionMatrix,
            "confusion-metrix-details-preview",
            "No confusion matrix available."
        );

        timeDetails.textContent =
            buildMetadataLine(data.timeTaken, data.dateCreated) ||
            "Training completed.";

        if (Array.isArray(data.tblFeatureImportance) && data.tblFeatureImportance.length > 0) {
            featureImportanceSection.classList.remove("hidden");
            displayTable(
                data.tblFeatureImportance,
                "feature-importance-preview",
                "No feature importance output available."
            );
        } else if (featureImportanceSection && featureImportancePreview) {
            featureImportanceSection.classList.add("hidden");
            featureImportancePreview.innerHTML =
                '<p class="empty-state">No feature importance output available.</p>';
        }

        setButtonEnabled("btn-proceed-inference", true);
        setStatusMessage(
            "train-upload-status",
            "Training finished. The scoring workspace is now unlocked.",
            "success"
        );
        setStageStatus("stage-train-status", "Model trained", "success");
        setStageStatus("stage-score-status", "Ready for scoring", "neutral");
        showAppNotice(
            "Training finished. Review the metrics, then move to scoring.",
            "success"
        );
    } catch (error) {
        console.error("Training error:", error);
        stopProgress(progress, false);
        setStatusMessage(
            "train-upload-status",
            error.message || "Model training failed.",
            "danger"
        );
        showAppNotice(error.message || "Model training failed.", "danger");
    }
}

async function inferenceModel() {
    const progress = startProgress(
        "loading-spinner-inference-progress",
        "progress-bar-container-infer",
        "progress-bar-infer",
        "progress-label-infer"
    );
    const timeDetails = document.getElementById("inference-time-details");

    setStatusMessage("inference-run-status", "Scoring customers...", "neutral");
    showAppNotice("Running inference with the live scoring route...", "neutral");

    try {
        const data = await fetchJson(
            "/score/model",
            {
                method: "POST",
            },
            "Inference failed."
        );

        const rows = normalizeTableData(data.tblOutput);
        UI_STATE.scoredRows = rows;

        stopProgress(progress, true);
        displayTable(rows, "churn-report-result", "No scoring rows returned.");
        renderInferenceSummary(rows);

        timeDetails.textContent =
            buildMetadataLine(data.timeTaken, data.dateCreated) ||
            "Inference completed.";

        setStatusMessage(
            "inference-run-status",
            `${rows.length} scored customer row(s) returned by the backend.`,
            "success"
        );
        setStageStatus("stage-score-status", "Scoring complete", "success");
        setStageStatus("stage-email-status", "Preview ready", "warning");
        renderEmailPlaceholderState(
            "Scored customers are ready. Preview the email workspace to stage the future backend flow."
        );
        showAppNotice(
            "Inference complete. Review scored customers and the email placeholder lane.",
            "success"
        );
    } catch (error) {
        console.error("Inference error:", error);
        stopProgress(progress, false);
        setStatusMessage(
            "inference-run-status",
            error.message || "Inference failed.",
            "danger"
        );
        showAppNotice(error.message || "Inference failed.", "danger");
    }
}

function createEmail() {
    const rows = normalizeTableData(UI_STATE.scoredRows);
    const container = document.getElementById("email-generation-result");

    if (!container) {
        return;
    }

    if (rows.length === 0) {
        renderEmailPlaceholderState(
            "No scored customers are loaded yet. Run inference before previewing the email lane."
        );
        setStatusMessage(
            "email-placeholder-status",
            "Run inference first to stage the future email workspace.",
            "warning"
        );
        showAppNotice(
            "Email preview is unavailable until scored customers exist.",
            "warning"
        );
        return;
    }

    const highRiskRows = rows.filter((row) => isTruthyPrediction(row.Prediction));
    const averageProbability = computeAverageProbability(highRiskRows);

    container.innerHTML = "";

    const summaryGrid = document.createElement("div");
    summaryGrid.className = "summary-grid";
    summaryGrid.appendChild(
        buildSummaryCard("Scored customers", `${rows.length}`, "Rows ready for future email staging.")
    );
    summaryGrid.appendChild(
        buildSummaryCard(
            "Churn risk",
            `${highRiskRows.length}`,
            "Customers currently flagged for retention outreach."
        )
    );
    summaryGrid.appendChild(
        buildSummaryCard(
            "Average risk",
            highRiskRows.length ? formatPercentage(averageProbability) : "0.0%",
            "Based on churn-positive customers in the latest scoring run."
        )
    );

    container.appendChild(summaryGrid);

    if (highRiskRows.length === 0) {
        const emptyMessage = document.createElement("p");
        emptyMessage.className = "empty-state";
        emptyMessage.textContent =
            "The latest scoring run did not flag any customers as churn risk, so there is nothing to stage for future email generation yet.";
        container.appendChild(emptyMessage);

        setStatusMessage(
            "email-placeholder-status",
            "Scoring completed, but no churn-risk customers are staged for email preview.",
            "neutral"
        );
        setStatusMessage(
            "send-email-status",
            "Send Emails remains disabled until the backend delivery flow exists.",
            "warning"
        );
        showAppNotice(
            "Email preview refreshed. No churn-risk customers were found in the latest scoring run.",
            "neutral"
        );
        return;
    }

    const previewList = document.createElement("div");
    previewList.className = "email-preview-list";

    highRiskRows.slice(0, 3).forEach((row) => {
        previewList.appendChild(buildEmailPreviewCard(row));
    });

    container.appendChild(previewList);

    setStatusMessage(
        "email-placeholder-status",
        `${highRiskRows.length} churn-risk customer(s) are staged for future email generation.`,
        "warning"
    );
    setStatusMessage(
        "send-email-status",
        "Send Emails remains disabled until the backend delivery flow exists.",
        "warning"
    );
    setStageStatus("stage-email-status", "Placeholder staged", "warning");
    showAppNotice(
        "Email workspace refreshed. These previews are frontend placeholders, not backend-generated drafts.",
        "warning"
    );
}

async function viewResults() {
    const spinner = document.getElementById("loading-spinner-view-results");
    const rowCountDiv = document.getElementById("view-result-result-row-count");
    const strTableName = document.getElementById("table-type")?.value;
    const strTableVersion = document.getElementById("version-type")?.value;

    if (!spinner || !rowCountDiv || !strTableName || !strTableVersion) {
        return;
    }

    spinner.style.display = "block";
    rowCountDiv.className = "inline-status tone-neutral";
    rowCountDiv.textContent = "Loading table data...";

    try {
        const query = new URLSearchParams({
            strTableName,
            strTableVersion,
        });

        const data = await fetchJson(
            `/database/table?${query.toString()}`,
            {
                method: "GET",
            },
            "Unable to load results."
        );

        UI_STATE.resultsRows = normalizeTableData(data.tblOutput);
        rowCountDiv.textContent = `Row count: ${data.intRowCount}`;
        displayTable(
            data.tblOutput,
            "view-result-result",
            "The selected table does not contain any rows."
        );
        showAppNotice(
            `Loaded ${data.intRowCount} row(s) from the ${strTableVersion} ${strTableName} table.`,
            "success"
        );
    } catch (error) {
        console.error("View results error:", error);
        rowCountDiv.className = "inline-status tone-danger";
        rowCountDiv.textContent = error.message || "Unable to load results.";
        document.getElementById("view-result-result").innerHTML =
            '<p class="empty-state">No result table loaded.</p>';
        showAppNotice(error.message || "Unable to load results.", "danger");
    } finally {
        spinner.style.display = "none";
    }
}

function sendEmail() {
    setStatusMessage(
        "send-email-status",
        "Email sending is intentionally disabled. Build the backend delivery service before enabling this control.",
        "warning"
    );
    showAppNotice(
        "Send Emails is a placeholder only. The backend delivery endpoint does not exist in the current FastAPI app.",
        "warning"
    );
}

function renderInferenceSummary(rows) {
    const container = document.getElementById("scoring-summary-cards");
    if (!container) {
        return;
    }

    const records = normalizeTableData(rows);
    const highRiskRows = records.filter((row) => isTruthyPrediction(row.Prediction));
    const averageProbability = computeAverageProbability(records);
    const maxProbability = records.reduce((highest, row) => {
        const current = Number(row.Churn_Probability) || 0;
        return Math.max(highest, current);
    }, 0);

    container.innerHTML = "";
    container.appendChild(
        buildSummaryCard(
            "Customers scored",
            `${records.length}`,
            "Rows returned by the latest scoring run."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Churn-positive",
            `${highRiskRows.length}`,
            "Customers flagged for retention follow-up."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Average probability",
            records.length ? formatPercentage(averageProbability) : "0.0%",
            "Average churn probability across the returned batch."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Highest probability",
            records.length ? formatPercentage(maxProbability) : "0.0%",
            "Peak churn risk in the current scoring batch."
        )
    );
}

function renderKeyValueCards(data, targetId) {
    const container = document.getElementById(targetId);
    if (!container) {
        return;
    }

    const records = normalizeTableData(data);
    const firstRow = records[0];

    container.innerHTML = "";

    if (!firstRow) {
        container.innerHTML = '<p class="empty-state">No data available yet.</p>';
        return;
    }

    Object.entries(firstRow).forEach(([key, value]) => {
        container.appendChild(
            buildSummaryCard(
                humanizeLabel(key),
                formatSummaryValue(key, value),
                "Latest backend response."
            )
        );
    });
}

function renderEmailPlaceholderState(message) {
    const container = document.getElementById("email-generation-result");
    if (!container) {
        return;
    }

    container.innerHTML = `
        <div class="placeholder-grid">
            <div class="placeholder-card">
                <strong>What will land later</strong>
                <p>
                    Personalized draft generation for churn-risk customers using their
                    top churn drivers and profile context.
                </p>
            </div>
            <div class="placeholder-card">
                <strong>What stays disabled now</strong>
                <p>
                    Bulk sending, delivery safety checks, and backend queueing are not
                    available in the current FastAPI app.
                </p>
            </div>
        </div>
        <p class="empty-state">${message}</p>
    `;
}

function buildSummaryCard(label, value, description) {
    const card = document.createElement("div");
    card.className = "summary-card";
    card.innerHTML = `
        <span class="kicker">${escapeHtml(label)}</span>
        <strong>${escapeHtml(String(value))}</strong>
        <p>${escapeHtml(description)}</p>
    `;
    return card;
}

function buildEmailPreviewCard(row) {
    const card = document.createElement("article");
    card.className = "email-preview-card";

    const surname = row.Surname || "Customer";
    const email = row.Email || "Unknown recipient";
    const topDrivers = [
        row.Top_1_Feat && row.Top_1_Feat_Value
            ? `${humanizeLabel(row.Top_1_Feat)}: ${row.Top_1_Feat_Value}`
            : null,
        row.Top_2_Feat && row.Top_2_Feat_Value
            ? `${humanizeLabel(row.Top_2_Feat)}: ${row.Top_2_Feat_Value}`
            : null,
        row.Top_3_Feat && row.Top_3_Feat_Value
            ? `${humanizeLabel(row.Top_3_Feat)}: ${row.Top_3_Feat_Value}`
            : null,
    ].filter(Boolean);

    const probabilityText = formatPercentage(Number(row.Churn_Probability) || 0);
    const subjectLine = `Subject: A better plan for ${surname}'s next banking moment`;

    card.innerHTML = `
        <div class="email-preview-header">
            <span class="email-preview-recipient">${escapeHtml(email)}</span>
            <span class="pill">Draft placeholder</span>
        </div>
        <h4>${escapeHtml(subjectLine)}</h4>
        <p>Hi ${escapeHtml(surname)},</p>
        <p>
            This preview is standing in for the backend email generator. When that
            service is restored, it can tailor retention copy from the churn signals
            already produced by scoring.
        </p>
        <p>
            Current risk snapshot: ${escapeHtml(probabilityText)} churn probability.
            Key drivers: ${escapeHtml(topDrivers.join(" | ") || "Scored factors pending.")}.
        </p>
        <p class="email-footnote">
            Delivery is intentionally disabled until the send-email backend is built.
        </p>
    `;

    return card;
}

function displayTable(data, targetId, emptyMessage = "No data to display.") {
    const container = document.getElementById(targetId);
    const rows = normalizeTableData(data);

    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (rows.length === 0) {
        container.innerHTML = `<p class="empty-state">${escapeHtml(emptyMessage)}</p>`;
        return;
    }

    const header = Object.keys(rows[0]);
    const toolbar = document.createElement("div");
    toolbar.className = "table-toolbar";

    const meta = document.createElement("p");
    meta.className = "table-meta";
    meta.textContent = `${rows.length} row(s)`;

    const downloadButton = document.createElement("button");
    downloadButton.type = "button";
    downloadButton.className = "table-download-button";
    downloadButton.textContent = "Download CSV";
    downloadButton.addEventListener("click", () => {
        downloadRowsAsCsv(rows, `${targetId}.csv`);
    });

    toolbar.append(meta, downloadButton);

    const tableScroll = document.createElement("div");
    tableScroll.className = "table-scroll";

    const table = document.createElement("table");
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");

    header.forEach((key) => {
        const th = document.createElement("th");
        th.textContent = humanizeLabel(key);
        headerRow.appendChild(th);
    });

    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    rows.forEach((row) => {
        const tr = document.createElement("tr");
        header.forEach((key) => {
            const td = document.createElement("td");
            td.textContent = formatTableValue(row[key]);
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    tableScroll.appendChild(table);

    container.append(toolbar, tableScroll);
}

function downloadRowsAsCsv(rows, filename) {
    if (!rows.length) {
        return;
    }

    const header = Object.keys(rows[0]);
    const csvRows = [header.join(",")];

    rows.forEach((row) => {
        const values = header.map((key) =>
            `"${String(row[key] ?? "").replace(/"/g, '""')}"`
        );
        csvRows.push(values.join(","));
    });

    const csvContent = csvRows.join("\n");
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const link = document.createElement("a");
    const url = URL.createObjectURL(blob);

    link.href = url;
    link.setAttribute("download", filename);
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
}

function computeAverageProbability(rows) {
    const records = normalizeTableData(rows);
    if (!records.length) {
        return 0;
    }

    const total = records.reduce((sum, row) => {
        return sum + (Number(row.Churn_Probability) || 0);
    }, 0);

    return total / records.length;
}

function isTruthyPrediction(value) {
    if (typeof value === "boolean") {
        return value;
    }
    if (typeof value === "number") {
        return value === 1;
    }
    return String(value).toLowerCase() === "true" || String(value) === "1";
}

function humanizeLabel(value) {
    return String(value)
        .replace(/^meta_/, "")
        .replace(/^(int|flt|str|obj|tbl)/, "")
        .replace(/_/g, " ")
        .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
        .replace(/\bId\b/g, "ID")
        .trim();
}

function formatSummaryValue(key, value) {
    const loweredKey = String(key).toLowerCase();

    if (typeof value === "boolean") {
        return value ? "True" : "False";
    }

    if (typeof value === "number") {
        if (
            /(accuracy|precision|recall|f1|probability|score)/.test(loweredKey) &&
            value >= 0 &&
            value <= 1
        ) {
            return formatPercentage(value);
        }

        if (Number.isInteger(value)) {
            return value.toLocaleString();
        }

        return value.toFixed(3);
    }

    if (value === null || value === undefined || value === "") {
        return "—";
    }

    return String(value);
}

function formatTableValue(value) {
    if (value === null || value === undefined || value === "") {
        return "—";
    }

    if (typeof value === "boolean") {
        return value ? "True" : "False";
    }

    if (typeof value === "number") {
        if (Number.isInteger(value)) {
            return value.toLocaleString();
        }

        return value.toLocaleString(undefined, {
            maximumFractionDigits: 4,
        });
    }

    return String(value);
}

function formatPercentage(value) {
    return `${(Number(value) * 100).toFixed(1)}%`;
}

function capitalize(value) {
    return String(value).charAt(0).toUpperCase() + String(value).slice(1);
}

function escapeHtml(value) {
    return String(value)
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
}
