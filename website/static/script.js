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

const MAX_TABLE_RENDER_ROWS = 10000;
const TABLE_VISIBLE_ROWS = 20;
const MODEL_LAB_STEP_META = {
    load: {
        title: "Navigation Pane: Model Lab -> Load Training",
        subtitle:
            "Upload the training CSV and inspect the dataset before running the current backend training pipeline.",
    },
    run: {
        title: "Navigation Pane: Model Lab -> Run Training",
        subtitle:
            "Use the live training route here. Optional model-family tuning tiles stay in placeholder mode until the backend exposes them.",
    },
    result: {
        title: "Navigation Pane: Model Lab -> Training Result",
        subtitle:
            "Review the best available model snapshot and compare it against historical training runs stored in the database.",
    },
};

const UI_STATE = {
    trainingRows: [],
    scoringRows: [],
    scoredRows: [],
    resultsRows: [],
    modelLabStep: "load",
    lastTrainingResponse: null,
    modelHistoryRows: [],
    modelHistoryLoaded: false,
};

document.addEventListener("DOMContentLoaded", () => {
    initializeUi();
});

function initializeUi() {
    buildModelLabWorkspace();
    showSection("train-section");
    showModelLabStep("load");
    initializeFileInputs();
    initializeResultsFilters();
    renderTrainingDatasetProfile([]);
    renderTrainingResultPlaceholders();
    renderEmailPlaceholderState(
        "Run inference first, then preview how the future email lane will be staged."
    );
    loadModelHistory().catch((error) => {
        console.error("Model history preload error:", error);
    });
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

function buildModelLabWorkspace() {
    const root = document.getElementById("model-lab-stage-root");
    if (!root || root.dataset.ready === "true") {
        return;
    }

    root.innerHTML = `
        <div id="model-step-load" class="model-step-view">
            <div class="model-grid-load">
                <article class="panel stage-card stage-card-a">
                    <div class="panel-header panel-header-stack">
                        <div>
                            <span class="stage-index">A.</span>
                            <h3>Dataset Preview</h3>
                            <p>The uploaded training dataset is rendered here exactly as returned by the backend upload route.</p>
                        </div>
                        <div class="panel-actions">
                            <div id="model-lab-load-input-slot"></div>
                            <button type="button" class="primary-button" onclick="uploadCSV('train')">Upload Training CSV</button>
                        </div>
                    </div>
                    <div id="model-lab-load-status-slot" class="inline-stack"></div>
                    <div id="model-lab-load-preview-slot"></div>
                </article>
                <article class="panel stage-card stage-card-c">
                    <div class="panel-header">
                        <div>
                            <span class="stage-index">C.</span>
                            <h3>Dataset Split &amp; Feature Summary</h3>
                            <p>Dataset profiling appears after upload. Training split details appear after a model run.</p>
                        </div>
                    </div>
                    <div id="train-dataset-summary" class="summary-stack"></div>
                </article>
                <article class="panel stage-card stage-card-b">
                    <div class="panel-header">
                        <div>
                            <span class="stage-index">B.</span>
                            <h3>Feature Preview</h3>
                            <p>Frontend-generated field profile based on the uploaded training dataset.</p>
                        </div>
                    </div>
                    <div id="train-feature-preview" class="table-shell table-shell-medium"></div>
                </article>
            </div>
        </div>
        <div id="model-step-run" class="model-step-view hidden">
            <div class="tuning-grid">
                <article class="panel tuning-card">
                    <span class="placeholder-badge">Backend placeholder</span>
                    <h3>Optional Step 1: Parameter Tuning</h3>
                    <p class="tuning-title">Random Forest</p>
                    <p>The current backend does not expose model-family selection or Random Forest hyperparameters yet.</p>
                </article>
                <article class="panel tuning-card">
                    <span class="placeholder-badge">Backend placeholder</span>
                    <h3>Optional Step 2: Parameter Tuning</h3>
                    <p class="tuning-title">Logistic Regression</p>
                    <p>This model option is represented in the UI, but there is no route for training or tuning it today.</p>
                </article>
                <article class="panel tuning-card">
                    <span class="placeholder-badge">Backend placeholder</span>
                    <h3>Optional Step 3: Parameter Tuning</h3>
                    <p class="tuning-title">Linear Regression</p>
                    <p>Kept as a placeholder panel until the backend exposes additional training pipelines.</p>
                </article>
            </div>
            <article class="panel stage-card">
                <div class="panel-header panel-header-stack">
                    <div>
                        <span class="stage-index">A.</span>
                        <h3>Model Training Preview</h3>
                        <p>This view runs the current training endpoint and stages the response in one operational panel.</p>
                    </div>
                    <div class="action-row">
                        <button type="button" class="secondary-button" onclick="showModelLabStep('result')">Open Training Result</button>
                    </div>
                </div>
                <div class="training-preview-layout">
                    <div class="training-controls">
                        <div class="info-banner">The current backend route runs one stored pipeline. The tuning tiles above remain placeholders until separate model routes exist.</div>
                        <div id="model-lab-run-controls-slot" class="inline-stack"></div>
                    </div>
                    <div class="training-preview-panel">
                        <div class="result-detail-panel">
                            <h4>Run Snapshot</h4>
                            <div id="model-lab-run-metrics-slot"></div>
                        </div>
                        <div class="result-detail-panel">
                            <h4>Execution Notes</h4>
                            <div class="placeholder-grid placeholder-grid-compact">
                                <div class="placeholder-card">
                                    <strong>Live backend route</strong>
                                    <p>POST /train/model remains the source of truth for this panel.</p>
                                </div>
                                <div class="placeholder-card">
                                    <strong>Planned later</strong>
                                    <p>Multi-model comparisons and algorithm-specific tuning stay placeholder-only for now.</p>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </article>
        </div>
        <div id="model-step-result" class="model-step-view hidden">
            <div class="model-grid-result">
                <article class="panel stage-card">
                    <div class="panel-header panel-header-stack">
                        <div>
                            <span class="stage-index">A.</span>
                            <h3>Best Model Preview</h3>
                            <p>Best available model snapshot derived from the latest training response and the historical models table.</p>
                        </div>
                        <div id="model-lab-result-action-slot" class="action-row"></div>
                    </div>
                    <p id="best-model-runtime" class="meta-line">No trained model snapshot yet.</p>
                    <div id="best-model-highlight-cards" class="summary-grid"></div>
                    <div class="result-detail-grid">
                        <div class="result-detail-panel">
                            <h4>Dataset Split</h4>
                            <div id="model-lab-result-split-slot"></div>
                        </div>
                        <div class="result-detail-panel">
                            <h4>Model Metrics</h4>
                            <div id="model-lab-result-metrics-slot"></div>
                        </div>
                        <div class="result-detail-panel">
                            <h4>Confusion Matrix</h4>
                            <div id="model-lab-result-confusion-slot"></div>
                        </div>
                        <div class="result-detail-panel">
                            <h4>Feature Importance</h4>
                            <div id="model-lab-result-feature-slot"></div>
                        </div>
                    </div>
                </article>
                <article class="panel stage-card">
                    <div class="panel-header">
                        <div>
                            <span class="stage-index">B.</span>
                            <h3>Other Models Preview</h3>
                            <p>Historical models from the database appear here after at least two runs exist.</p>
                        </div>
                    </div>
                    <div id="historical-models-preview" class="table-shell table-shell-large"></div>
                </article>
            </div>
        </div>
    `;

    moveNodeToSlot(document.querySelector('label[for="train-file"]'), "model-lab-load-input-slot");
    moveNodeToSlot(document.getElementById("train-file-name"), "model-lab-load-status-slot");
    moveNodeToSlot(document.getElementById("train-upload-status"), "model-lab-load-status-slot");
    moveNodeToSlot(document.getElementById("uploaded-train-preview"), "model-lab-load-preview-slot");
    moveNodeToSlot(document.querySelector("#train-section .input-grid"), "model-lab-run-controls-slot");
    moveNodeToSlot(document.querySelector("#train-section .workflow-grid .step-card:nth-of-type(2) .action-row"), "model-lab-run-controls-slot");
    moveNodeToSlot(document.getElementById("time-details"), "model-lab-run-controls-slot");
    moveNodeToSlot(document.getElementById("progress-bar-container-train"), "model-lab-run-controls-slot");
    moveNodeToSlot(document.getElementById("training-metric-cards"), "model-lab-run-metrics-slot");
    moveNodeToSlot(document.getElementById("btn-proceed-inference"), "model-lab-result-action-slot");
    moveNodeToSlot(document.getElementById("training-details-preview"), "model-lab-result-split-slot");
    moveNodeToSlot(document.getElementById("metrics-details-preview"), "model-lab-result-metrics-slot");
    moveNodeToSlot(document.getElementById("confusion-metrix-details-preview"), "model-lab-result-confusion-slot");
    moveNodeToSlot(document.getElementById("feature-importance-preview"), "model-lab-result-feature-slot");

    root.dataset.ready = "true";
}

function moveNodeToSlot(node, slotId) {
    const slot = document.getElementById(slotId);
    if (!node || !slot) {
        return;
    }
    slot.appendChild(node);
}

function showSection(sectionId) {
    document.querySelectorAll(".section").forEach((section) => {
        section.classList.add("hidden");
    });

    document.getElementById(sectionId)?.classList.remove("hidden");

    if (sectionId === "train-section") {
        showModelLabStep(UI_STATE.modelLabStep || "load");
    }

    document.querySelectorAll(".nav-button").forEach((button) => {
        const isActive = button.dataset.sectionTarget === sectionId;
        button.classList.toggle("is-active", isActive);
    });
}

function showModelLabStep(stepId) {
    UI_STATE.modelLabStep = MODEL_LAB_STEP_META[stepId] ? stepId : "load";

    document.querySelectorAll("#model-lab-stage-root .model-step-view").forEach((view) => {
        view.classList.add("hidden");
    });
    document.getElementById(`model-step-${UI_STATE.modelLabStep}`)?.classList.remove("hidden");

    document.querySelectorAll(".lab-step-button").forEach((button) => {
        button.classList.toggle(
            "is-active",
            button.dataset.modelStepTarget === UI_STATE.modelLabStep
        );
    });

    const meta = MODEL_LAB_STEP_META[UI_STATE.modelLabStep];
    const title = document.getElementById("model-lab-title");
    const subtitle = document.getElementById("model-lab-subtitle");

    if (title) {
        title.textContent = meta.title;
    }
    if (subtitle) {
        subtitle.textContent = meta.subtitle;
    }

    if (UI_STATE.modelLabStep === "result") {
        loadModelHistory().catch((error) => {
            console.error("Model history load error:", error);
        });
    }
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
            UI_STATE.lastTrainingResponse = null;
            renderTrainingDatasetProfile(rows);
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
        if (type === "train") {
            showModelLabStep("load");
        }
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
        UI_STATE.lastTrainingResponse = data;

        renderKeyValueCards(data.objMetrics, "training-metric-cards");
        renderMetricGrid(
            data.objDatasetSplit,
            "training-details-preview",
            "No dataset split available."
        );
        renderMetricGrid(data.objMetrics, "metrics-details-preview", "No model metrics available.");
        renderMetricGrid(
            data.objConfusionMatrix,
            "confusion-metrix-details-preview",
            "No confusion matrix available."
        );

        timeDetails.textContent =
            buildMetadataLine(data.timeTaken, data.dateCreated) ||
            "Training completed.";

        if (Array.isArray(data.tblFeatureImportance) && data.tblFeatureImportance.length > 0) {
            displayTable(
                data.tblFeatureImportance,
                "feature-importance-preview",
                "No feature importance output available."
            );
        } else if (featureImportancePreview) {
            featureImportancePreview.innerHTML =
                '<p class="empty-state">The current backend training response does not provide feature importance yet.</p>';
        }

        renderTrainingDatasetProfile(UI_STATE.trainingRows, data.objDatasetSplit);
        renderLatestTrainingResultSummary(data);
        await loadModelHistory(true);
        setButtonEnabled("btn-proceed-inference", true);
        setStatusMessage(
            "train-upload-status",
            "Training finished. The scoring workspace is now unlocked.",
            "success"
        );
        setStageStatus("stage-train-status", "Model trained", "success");
        setStageStatus("stage-score-status", "Ready for scoring", "neutral");
        showModelLabStep("result");
        showAppNotice(
            "Training finished. Review the result pane, then move to scoring.",
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

function renderTrainingDatasetProfile(rows, datasetSplit = null) {
    const records = normalizeTableData(rows);
    const profile = buildDatasetProfile(records);
    const summaryContainer = document.getElementById("train-dataset-summary");

    if (!summaryContainer) {
        return;
    }

    summaryContainer.innerHTML = "";

    if (!profile) {
        summaryContainer.appendChild(
            buildSummaryCard(
                "Waiting",
                "No dataset yet",
                "Upload a training CSV to build the summary pane."
            )
        );
        renderTrainingFeatureTable([]);
        return;
    }

    renderTrainingFeatureTable(profile.fields);

    summaryContainer.appendChild(
        buildSummaryCard("Rows loaded", profile.rowCount.toLocaleString(), "Records currently staged for training.")
    );
    summaryContainer.appendChild(
        buildSummaryCard("Fields", profile.fieldCount.toLocaleString(), "Columns detected from the uploaded dataset.")
    );
    summaryContainer.appendChild(
        buildSummaryCard("Numeric fields", profile.numericCount.toLocaleString(), "Detected from non-empty column values.")
    );
    summaryContainer.appendChild(
        buildSummaryCard("Categorical fields", profile.categoricalCount.toLocaleString(), "Columns treated as discrete values.")
    );
    summaryContainer.appendChild(
        buildSummaryCard(
            "Target column",
            profile.targetIncluded ? "Exited" : "Missing",
            profile.targetIncluded
                ? "The expected churn label is available in the uploaded training set."
                : "The expected churn label was not found in the uploaded training set."
        )
    );

    if (datasetSplit) {
        const splitRow = normalizeTableData(datasetSplit)[0] || {};
        const trainTotal =
            (Number(splitRow.intNegativeTraining) || 0) +
            (Number(splitRow.intPositiveTraining) || 0);
        const testTotal =
            (Number(splitRow.intNegativeTesting) || 0) +
            (Number(splitRow.intPositiveTesting) || 0);

        summaryContainer.appendChild(
            buildSummaryCard(
                "Dataset split",
                `${trainTotal.toLocaleString()} / ${testTotal.toLocaleString()}`,
                `Train positives ${Number(splitRow.intPositiveTraining || 0).toLocaleString()} and test positives ${Number(splitRow.intPositiveTesting || 0).toLocaleString()}.`
            )
        );
    } else {
        summaryContainer.appendChild(
            buildSummaryCard(
                "Dataset split",
                "Pending training",
                "Run the training step to populate the backend train/test split summary."
            )
        );
    }
}

function renderTrainingFeatureTable(fields) {
    const rows = normalizeTableData(fields);
    if (!rows.length) {
        const container = document.getElementById("train-feature-preview");
        if (container) {
            container.innerHTML =
                '<p class="empty-state">Upload a training CSV to inspect its columns and fill rates.</p>';
        }
        return;
    }

    displayTable(rows, "train-feature-preview", "No field profile available.");
}

function buildDatasetProfile(rows) {
    const records = normalizeTableData(rows);
    if (!records.length) {
        return null;
    }

    const columns = Object.keys(records[0]);
    const fieldRows = columns.map((columnName) => {
        const values = records
            .map((row) => row[columnName])
            .filter((value) => value !== null && value !== undefined && value !== "");
        const sample = values.length ? values[0] : null;
        const uniqueCount = new Set(values.map((value) => String(value))).size;
        const filledPercent = Math.round((values.length / records.length) * 100);
        const columnType = detectColumnType(values);

        return {
            Field: humanizeLabel(columnName),
            Role: columnName === "Exited" ? "Target" : "Feature",
            Type: columnType,
            Filled: `${filledPercent}%`,
            UniqueValues: uniqueCount.toLocaleString(),
            Sample: formatTableValue(sample),
        };
    });

    const numericCount = fieldRows.filter((row) => row.Type !== "Categorical").length;

    return {
        rowCount: records.length,
        fieldCount: columns.length,
        numericCount,
        categoricalCount: columns.length - numericCount,
        targetIncluded: columns.includes("Exited"),
        fields: fieldRows,
    };
}

function detectColumnType(values) {
    if (!values.length) {
        return "Unknown";
    }

    const numericValues = values.filter((value) => isFiniteNumberish(value)).length;
    if (numericValues === values.length) {
        return "Numeric";
    }
    if (numericValues / values.length >= 0.85) {
        return "Mostly numeric";
    }
    return "Categorical";
}

function isFiniteNumberish(value) {
    if (typeof value === "number") {
        return Number.isFinite(value);
    }
    if (typeof value !== "string" || value.trim() === "") {
        return false;
    }
    return !Number.isNaN(Number(value));
}

function renderTrainingResultPlaceholders() {
    renderTrainingFeatureTable([]);
    renderTrainingDatasetProfile([], null);
    renderBestModelPlaceholder(
        "Run training or load historical model data to populate this view."
    );
    renderHistoricalModelsPlaceholder(
        "No additional historical models are available yet."
    );
}

function renderLatestTrainingResultSummary(data) {
    const container = document.getElementById("best-model-highlight-cards");
    const runtime = document.getElementById("best-model-runtime");
    if (!container || !runtime) {
        return;
    }

    container.innerHTML = "";
    runtime.textContent =
        buildMetadataLine(data.timeTaken, data.dateCreated) ||
        "Training completed.";

    const metrics = normalizeTableData(data.objMetrics)[0] || {};
    container.appendChild(
        buildSummaryCard("Source", "Latest run", "Directly returned by the training endpoint.")
    );
    container.appendChild(
        buildSummaryCard("F1", formatSummaryValue("fltF1", metrics.fltF1), "Primary score from the latest run.")
    );
    container.appendChild(
        buildSummaryCard(
            "Accuracy",
            formatSummaryValue("fltAccuracy", metrics.fltAccuracy),
            "Overall accuracy from the latest run."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Precision / Recall",
            `${formatSummaryValue("fltPrecision", metrics.fltPrecision)} / ${formatSummaryValue("fltRecall", metrics.fltRecall)}`,
            "Latest balance between positive precision and recall."
        )
    );
}

async function loadModelHistory(force = false) {
    if (UI_STATE.modelHistoryLoaded && !force) {
        renderHistoricalTrainingResults(UI_STATE.modelHistoryRows);
        return UI_STATE.modelHistoryRows;
    }

    try {
        const query = new URLSearchParams({
            strTableName: "models",
            strTableVersion: "historical",
        });
        const data = await fetchJson(
            `/database/table?${query.toString()}`,
            { method: "GET" },
            "Unable to load historical models."
        );

        UI_STATE.modelHistoryRows = normalizeTableData(data.tblOutput);
        UI_STATE.modelHistoryLoaded = true;
        renderHistoricalTrainingResults(UI_STATE.modelHistoryRows);
        return UI_STATE.modelHistoryRows;
    } catch (error) {
        UI_STATE.modelHistoryRows = [];
        UI_STATE.modelHistoryLoaded = true;
        renderHistoricalModelsPlaceholder(
            "Historical model data is unavailable until the backend stores at least one run."
        );
        if (!UI_STATE.lastTrainingResponse) {
            renderBestModelPlaceholder(
                "Run training or load historical model data to populate this view."
            );
        }
        throw error;
    }
}

function renderHistoricalTrainingResults(rows) {
    const records = normalizeTableData(rows);

    if (!records.length) {
        if (!UI_STATE.lastTrainingResponse) {
            renderBestModelPlaceholder(
                "Run training or load historical model data to populate this view."
            );
        }
        renderHistoricalModelsPlaceholder(
            "No additional historical models are available yet."
        );
        return;
    }

    const bestRecord = selectBestModelRecord(records);
    if (bestRecord) {
        renderBestModelFromHistory(bestRecord, records.length);
    }

    const otherModels = records.filter((row) => row.meta_Id !== bestRecord?.meta_Id);
    if (otherModels.length) {
        displayTable(
            otherModels,
            "historical-models-preview",
            "No additional historical models are available yet."
        );
    } else {
        renderHistoricalModelsPlaceholder(
            "Only one stored model run exists so far. Additional runs will appear here."
        );
    }

    setButtonEnabled("btn-proceed-inference", true);
}

function selectBestModelRecord(rows) {
    return [...normalizeTableData(rows)].sort((left, right) => {
        const f1Delta = (Number(right.F1) || 0) - (Number(left.F1) || 0);
        if (f1Delta !== 0) {
            return f1Delta;
        }

        const accuracyDelta =
            (Number(right.Accuracy) || 0) - (Number(left.Accuracy) || 0);
        if (accuracyDelta !== 0) {
            return accuracyDelta;
        }

        return new Date(right.meta_DateCreated || 0) - new Date(left.meta_DateCreated || 0);
    })[0];
}

function renderBestModelFromHistory(row, totalRuns) {
    const container = document.getElementById("best-model-highlight-cards");
    const runtime = document.getElementById("best-model-runtime");
    if (!container || !runtime) {
        return;
    }

    runtime.textContent = `Selected from ${totalRuns.toLocaleString()} historical model run(s). Logged ${formatDateTime(row.meta_DateCreated)}.`;
    container.innerHTML = "";
    container.appendChild(
        buildSummaryCard("Source", "Historical best", "Selected by highest F1, then accuracy.")
    );
    container.appendChild(
        buildSummaryCard("F1", formatSummaryValue("F1", row.F1), "Best F1 score in the stored model history.")
    );
    container.appendChild(
        buildSummaryCard(
            "Accuracy",
            formatSummaryValue("Accuracy", row.Accuracy),
            "Accuracy recorded for the selected model run."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Precision / Recall",
            `${formatSummaryValue("Precision", row.Precision)} / ${formatSummaryValue("Recall", row.Recall)}`,
            "Stored precision and recall for the selected run."
        )
    );

    renderMetricGrid(
        [
            {
                TrainingNegative: row.CountTrainingNegativeClass,
                TrainingPositive: row.CountTrainingPositiveClass,
                TestingNegative: row.CountTestNegativeClass,
                TestingPositive: row.CountTestPositiveClass,
            },
        ],
        "training-details-preview",
        "No dataset split available."
    );
    renderMetricGrid(
        [
            {
                Accuracy: row.Accuracy,
                Precision: row.Precision,
                Recall: row.Recall,
                F1: row.F1,
            },
        ],
        "metrics-details-preview",
        "No model metrics available."
    );
    renderMetricGrid(
        [
            {
                TrueNegative: row.CountTrueNegative,
                FalsePositive: row.CountFalsePositive,
                FalseNegative: row.CountFalseNegative,
                TruePositive: row.CountTruePositive,
            },
        ],
        "confusion-metrix-details-preview",
        "No confusion matrix available."
    );

    if (!UI_STATE.lastTrainingResponse?.tblFeatureImportance?.length) {
        const featureContainer = document.getElementById("feature-importance-preview");
        if (featureContainer) {
            featureContainer.innerHTML =
                '<p class="empty-state">The backend does not store feature importance in the historical models table yet.</p>';
        }
    }
}

function renderBestModelPlaceholder(message) {
    const container = document.getElementById("best-model-highlight-cards");
    const runtime = document.getElementById("best-model-runtime");
    if (container) {
        container.innerHTML = "";
        container.appendChild(buildSummaryCard("Best model", "Pending run", message));
    }
    if (runtime) {
        runtime.textContent = "No trained model snapshot yet.";
    }
}

function renderHistoricalModelsPlaceholder(message) {
    const container = document.getElementById("historical-models-preview");
    if (!container) {
        return;
    }

    container.innerHTML = `<p class="empty-state">${escapeHtml(message)}</p>`;
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

function renderMetricGrid(data, targetId, emptyMessage = "No data available yet.") {
    const container = document.getElementById(targetId);
    if (!container) {
        return;
    }

    const records = normalizeTableData(data);
    const firstRow = records[0];

    container.innerHTML = "";

    if (!firstRow) {
        container.innerHTML = `
            <div class="metric-tile metric-tile-empty">
                <span class="metric-label">${escapeHtml(humanizeLabel(targetId))}</span>
                <strong class="metric-value">Pending run</strong>
                <p class="metric-helper">${escapeHtml(emptyMessage)}</p>
            </div>
        `;
        return;
    }

    Object.entries(firstRow).forEach(([key, value]) => {
        const tile = document.createElement("div");
        tile.className = "metric-tile";
        tile.innerHTML = `
            <span class="metric-label">${escapeHtml(humanizeLabel(key))}</span>
            <strong class="metric-value">${escapeHtml(formatSummaryValue(key, value))}</strong>
            <p class="metric-helper">Latest training response.</p>
        `;
        container.appendChild(tile);
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
    const allRows = normalizeTableData(data);
    const rows = allRows.slice(0, MAX_TABLE_RENDER_ROWS);
    const wasTruncated = allRows.length > MAX_TABLE_RENDER_ROWS;

    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (allRows.length === 0) {
        container.innerHTML = `<p class="empty-state">${escapeHtml(emptyMessage)}</p>`;
        return;
    }

    const header = Object.keys(rows[0]);
    const toolbar = document.createElement("div");
    toolbar.className = "table-toolbar";

    const meta = document.createElement("p");
    meta.className = "table-meta";
    meta.textContent = wasTruncated
        ? `Showing first ${rows.length.toLocaleString()} of ${allRows.length.toLocaleString()} row(s)`
        : `${rows.length.toLocaleString()} row(s)`;

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
    lockTableViewport(tableScroll, table, TABLE_VISIBLE_ROWS);
}

function lockTableViewport(tableScroll, table, visibleRows) {
    if (!tableScroll || !table) {
        return;
    }

    const header = table.querySelector("thead");
    const bodyRows = Array.from(table.querySelectorAll("tbody tr"));

    if (!header || bodyRows.length === 0) {
        tableScroll.style.removeProperty("height");
        tableScroll.style.removeProperty("max-height");
        return;
    }

    const rowsToMeasure = bodyRows.slice(0, visibleRows);
    const headerHeight = Math.ceil(header.getBoundingClientRect().height);
    const rowsHeight = rowsToMeasure.reduce((total, row) => {
        return total + Math.ceil(row.getBoundingClientRect().height);
    }, 0);

    const viewportHeight = headerHeight + rowsHeight + 2;
    tableScroll.style.height = `${viewportHeight}px`;
    tableScroll.style.maxHeight = `${viewportHeight}px`;
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
        return "N/A";
    }

    return String(value);
}

function formatTableValue(value) {
    if (value === null || value === undefined || value === "") {
        return "N/A";
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
