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
const COLUMN_REVIEW_VISIBLE_ROWS = 10;
const TRAINING_MODEL_OPTIONS = [
    {
        id: 3,
        inputId: "train-model-random-forest",
        label: "Random Forest",
    },
    {
        id: 2,
        inputId: "train-model-logistic-regression",
        label: "Logistic Regression",
    },
    {
        id: 1,
        inputId: "train-model-linear-regression",
        label: "Linear Regression",
    },
];
const TRAINING_GENERAL_SETTING_IDS = [
    "train-random-state",
    "train-train-test-split",
    "train-cross-fold",
    "train-primary-metric",
    "train-top-feats",
    ...TRAINING_MODEL_OPTIONS.map((option) => option.inputId),
];
const MODEL_LAB_STEP_META = {
    load: {
        title: "Navigation Pane: Model Lab -> Load Training",
        subtitle:
            "Upload the training CSV, review the prescribed column checks, and inspect the previews before running the current backend training pipeline.",
    },
    run: {
        title: "Navigation Pane: Model Lab -> Run Training",
        subtitle:
            "Choose the model families to run, launch training, and compare candidate speed and quality with the champion highlighted first.",
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
    trainingTargetColumn: null,
    trainingColumnInclusion: {},
    trainingSettingsConfirmed: false,
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
    initializeTrainingSettingInputs();
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

function initializeTrainingSettingInputs() {
    TRAINING_GENERAL_SETTING_IDS.forEach((id) => {
        const element = document.getElementById(id);
        if (!element || element.dataset.trainingSettingBound === "true") {
            return;
        }

        const handleChange = () => {
            handleTrainingSettingsChanged();
        };

        element.addEventListener("change", handleChange);
        if (element.tagName === "INPUT" && element.type === "number") {
            element.addEventListener("input", handleChange);
        }

        element.dataset.trainingSettingBound = "true";
    });

    refreshTrainingSettingsStatus();
    setButtonEnabled("btn-view-training-results", Boolean(UI_STATE.lastTrainingResponse));
}

function setTrainingSettingsLocked(locked) {
    TRAINING_GENERAL_SETTING_IDS.forEach((id) => {
        const element = document.getElementById(id);
        if (element) {
            element.disabled = Boolean(locked);
        }
    });

    const button = document.getElementById("btn-confirm-training-settings");
    if (!button) {
        return;
    }

    button.classList.add("primary-button");
    button.classList.remove("secondary-button");
    button.textContent = locked ? "Training Setting Confirmed" : "Confirm Training Setting";
}

function handleTrainingSettingsChanged() {
    UI_STATE.trainingSettingsConfirmed = false;
    renderTrainingDatasetProfile(
        UI_STATE.trainingRows,
        UI_STATE.lastTrainingResponse?.objDatasetSplit || null
    );
    refreshTrainingSettingsStatus();
}

function refreshTrainingSettingsStatus() {
    const selectedModels = getSelectedTrainingModelLabels();
    const hasRows = normalizeTableData(UI_STATE.trainingRows).length > 0;
    const isConfirmed = Boolean(UI_STATE.trainingSettingsConfirmed);

    setTrainingSettingsLocked(isConfirmed);

    if (!selectedModels.length) {
        setStatusMessage(
            "train-settings-status",
            "Select at least one model family before confirming the training setting.",
            "danger"
        );
        return;
    }

    if (isConfirmed) {
        setStatusMessage(
            "train-settings-status",
            hasRows
                ? "Training setting confirmed. Step 1 is now locked for this training run."
                : "Training setting confirmed. Step 1 is now locked while the current configuration is active.",
            "success"
        );
        return;
    }

    setStatusMessage(
        "train-settings-status",
        "Review the general setting values, then confirm them to lock Step 1 before training.",
        "neutral"
    );
}

function confirmTrainingSettings() {
    if (UI_STATE.trainingSettingsConfirmed) {
        showAppNotice("Training setting is already confirmed and Step 1 is locked.", "neutral");
        return;
    }

    if (!getSelectedTrainingModelIds().length) {
        setStatusMessage(
            "train-settings-status",
            "Select at least one model family before confirming the training setting.",
            "danger"
        );
        showAppNotice(
            "Select at least one model family before confirming the training setting.",
            "danger"
        );
        return;
    }

    UI_STATE.trainingSettingsConfirmed = true;
    renderTrainingDatasetProfile(
        UI_STATE.trainingRows,
        UI_STATE.lastTrainingResponse?.objDatasetSplit || null
    );
    refreshTrainingSettingsStatus();
    showAppNotice("Training setting confirmed.", "success");
}

function buildModelLabWorkspace() {
    const root = document.getElementById("model-lab-stage-root");
    if (!root || root.dataset.ready === "true") {
        return;
    }

    root.innerHTML = `
        <div id="model-step-load" class="model-step-view">
            <div class="model-load-flow">
                <div class="model-load-step-grid">
                    <article class="panel model-load-step-card">
                        <div class="model-load-step-heading">
                            <span class="model-load-step-kicker">Step 1</span>
                            <h3>Load Training</h3>
                            <p>Upload the training CSV into the current Model Lab workspace.</p>
                        </div>
                        <div class="model-load-input-actions">
                            <div id="model-lab-load-input-slot"></div>
                            <button type="button" class="primary-button" onclick="uploadCSV('train')">Upload Training CSV</button>
                        </div>
                        <div id="model-lab-load-status-slot" class="model-load-status-list"></div>
                    </article>
                    <article class="panel model-load-step-card model-load-step-card-wide">
                        <div class="model-load-step-heading">
                            <span class="model-load-step-kicker">Step 2</span>
                            <h3>Finalize Columns</h3>
                            <p>Review the prescribed column recommendations before continuing to the live training route.</p>
                        </div>
                        <div class="model-load-target-picker">
                            <label class="model-load-target-field" for="train-target-column">
                                <span>Target feature name</span>
                                <select
                                    id="train-target-column"
                                    onchange="handleTrainingTargetChange(this.value)"
                                    disabled
                                >
                                    <option value="">Select target column</option>
                                </select>
                            </label>
                            <p id="train-target-column-note" class="field-hint">
                                Upload a training CSV to choose which feature is the target.
                            </p>
                        </div>
                        <div id="model-lab-column-review-preview" class="table-shell table-shell-compact"></div>
                    </article>
                    <article class="panel model-load-step-card">
                        <div class="model-load-step-heading">
                            <span class="model-load-step-kicker">Step 3</span>
                            <h3>Go To Training</h3>
                            <p>Move to the run step after the dataset is staged and the column review is visible.</p>
                        </div>
                        <div id="model-lab-load-next-summary" class="summary-stack"></div>
                        <div class="action-row">
                            <button
                                type="button"
                                id="btn-proceed-model-run"
                                class="primary-button is-disabled"
                                onclick="showModelLabStep('run')"
                                data-disabled-title="Upload a training dataset first."
                                title="Upload a training dataset first."
                                disabled
                            >
                                Open Run Training
                            </button>
                        </div>
                    </article>
                </div>
                <div class="model-load-divider" aria-hidden="true">
                    <span>Preview Workspace</span>
                </div>
                <div class="model-load-preview-stack">
                    <article class="panel stage-card">
                        <div class="panel-header">
                            <div>
                                <span class="stage-index">A.</span>
                                <h3>Dataset Preview</h3>
                                <p>The uploaded training dataset is rendered here exactly as returned by the backend upload route.</p>
                            </div>
                        </div>
                        <div id="model-lab-load-preview-slot"></div>
                    </article>
                    <article class="panel stage-card">
                        <div class="panel-header">
                            <div>
                                <span class="stage-index">B.</span>
                                <h3>Feature Preview</h3>
                                <p>Frontend-generated field profile based on the uploaded training dataset.</p>
                            </div>
                        </div>
                        <div id="train-feature-preview" class="table-shell table-shell-medium"></div>
                    </article>
                    <article class="panel stage-card">
                        <div class="panel-header">
                            <div>
                                <span class="stage-index">C.</span>
                                <h3>Dataset Split &amp; Feature Preview Summary</h3>
                                <p>Dataset profiling appears after upload. Training split details appear after a model run.</p>
                            </div>
                        </div>
                        <div id="train-dataset-summary" class="summary-stack"></div>
                    </article>
                </div>
            </div>
        </div>
        <div id="model-step-run" class="model-step-view hidden">
            <div class="model-run-workspace">
                <div class="model-run-top-grid">
                    <article class="panel model-run-card model-run-card-settings">
                        <div class="model-run-card-header">
                            <h3>Step 1: General Setting</h3>
                        </div>
                        <div id="model-lab-run-controls-slot" class="model-run-settings-controls"></div>
                        <div class="model-run-settings-footer">
                            <button
                                type="button"
                                id="btn-confirm-training-settings"
                                class="primary-button"
                                onclick="confirmTrainingSettings()"
                            >
                                Confirm Training Setting
                            </button>
                            <p id="train-settings-status" class="inline-status tone-neutral">
                                Training Overview is synced to the live general setting values.
                            </p>
                        </div>
                    </article>

                    <article class="panel model-run-card model-run-card-training">
                        <div class="model-run-training-header">
                            <div>
                                <h3>Step 2: Run Training</h3>
                                <p class="model-run-result-copy">
                                    Run the selected model families, review the champion details here, then open the detailed result screen.
                                </p>
                            </div>
                        </div>

                        <div class="model-run-training-footer">
                            <div id="model-lab-run-runtime-slot" class="model-run-runtime-stack"></div>
                            <div class="model-run-insight-grid">
                                <div class="result-detail-panel model-run-insight-panel model-run-insight-panel-champion">
                                    <div class="model-run-insight-head">
                                        <span class="model-run-insight-kicker">Winner</span>
                                        <h4>Champion Model Details</h4>
                                        <p class="model-run-insight-copy">
                                            Review the latest winning model, its speed, and the core metrics that made it win.
                                        </p>
                                    </div>
                                    <div id="model-lab-run-champion-slot" class="summary-grid summary-grid-tight model-run-insight-cards"></div>
                                </div>
                                <div class="result-detail-panel model-run-insight-panel model-run-insight-panel-hyperparams">
                                    <div class="model-run-insight-head">
                                        <span class="model-run-insight-kicker">Tuning</span>
                                        <h4>Champion Hyperparameters</h4>
                                        <p class="model-run-insight-copy">
                                            Review the best parameter set returned for the current champion model.
                                        </p>
                                    </div>
                                    <div id="model-lab-run-hyperparams-slot" class="model-run-param-list"></div>
                                </div>
                            </div>
                            <div class="action-row model-run-training-actions">
                                <div id="model-lab-run-action-slot" class="model-run-action-slot"></div>
                                <button
                                    type="button"
                                    id="btn-view-training-results"
                                    class="secondary-button is-disabled"
                                    data-disabled-title="Complete training first."
                                    title="Complete training first."
                                    disabled
                                    onclick="showModelLabStep('result')"
                                >
                                    Open View Results
                                </button>
                            </div>
                        </div>
                    </article>
                </div>

                <div class="model-load-divider" aria-hidden="true">
                    <span>Preview Workspace</span>
                </div>

                <div class="model-run-preview-grid">
                    <article class="panel stage-card model-run-overview-card">
                        <div class="panel-header">
                            <div>
                                <span class="stage-index">A.</span>
                                <h3>Training Overview</h3>
                                <p>Review the active training setting values and the projected train/test split counts.</p>
                            </div>
                        </div>
                        <div id="model-lab-run-training-overview-slot" class="summary-grid summary-grid-tight model-run-overview-cards"></div>
                    </article>
                    <article class="panel stage-card model-run-leaderboard-card">
                        <div class="panel-header">
                            <div>
                                <span class="stage-index">B.</span>
                                <h3>Model Run Comparison</h3>
                                <p>Compare the latest selected models in a simple table of runtime and core metrics.</p>
                            </div>
                        </div>
                        <div id="model-lab-run-leaderboard" class="table-shell table-shell-large model-run-showcase-shell"></div>
                    </article>
                </div>
            </div>
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
                            <h3>Model Comparison</h3>
                            <p>Current training candidates appear here with their latest metrics and top rankings.</p>
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
    moveNodeToSlot(
        document.querySelector(
            "#train-section .workflow-grid .step-card:nth-of-type(2) .action-row .primary-button"
        ),
        "model-lab-run-action-slot"
    );
    moveNodeToSlot(document.getElementById("time-details"), "model-lab-run-runtime-slot");
    moveNodeToSlot(document.getElementById("progress-bar-container-train"), "model-lab-run-runtime-slot");
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

    requestAnimationFrame(() => {
        refreshTableViewports();
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

    requestAnimationFrame(() => {
        refreshTableViewports();
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

function normalizeFeatureImportanceRows(data) {
    if (!Array.isArray(data)) {
        return [];
    }

    const rows = data.length === 1 && Array.isArray(data[0]) ? data[0] : data;
    return rows.filter((row) => row && typeof row === "object" && !Array.isArray(row));
}

function normalizeTrainingModelResults(data) {
    if (!Array.isArray(data)) {
        return [];
    }

    return data
        .filter((row) => row && typeof row === "object" && !Array.isArray(row))
        .map((row) => ({
            strModelName:
                row.strModelName ||
                row.Model ||
                row.ModelName ||
                row.Name ||
                "Current backend pipeline",
            boolIsChampion: Boolean(row.boolIsChampion),
            fltGridScore: Number(row.fltGridScore) || 0,
            fltTimeTaken: Number(row.fltTimeTaken) || 0,
            dicBestParams:
                row.dicBestParams && typeof row.dicBestParams === "object"
                    ? row.dicBestParams
                    : {},
            objMetrics: normalizeTableData(row.objMetrics)[0] || {},
            objConfusionMatrix: normalizeTableData(row.objConfusionMatrix)[0] || {},
            tblFeatureImportance: normalizeFeatureImportanceRows(row.tblFeatureImportance),
        }));
}

function sortTrainingModelResults(results) {
    return [...normalizeTrainingModelResults(results)].sort((left, right) => {
        const championDelta = Number(Boolean(right.boolIsChampion)) - Number(Boolean(left.boolIsChampion));
        if (championDelta !== 0) {
            return championDelta;
        }

        const f1Delta =
            (Number(right.objMetrics?.fltF1) || 0) - (Number(left.objMetrics?.fltF1) || 0);
        if (f1Delta !== 0) {
            return f1Delta;
        }

        const accuracyDelta =
            (Number(right.objMetrics?.fltAccuracy) || 0) -
            (Number(left.objMetrics?.fltAccuracy) || 0);
        if (accuracyDelta !== 0) {
            return accuracyDelta;
        }

        return (Number(right.fltGridScore) || 0) - (Number(left.fltGridScore) || 0);
    });
}

function formatModelParams(params) {
    if (!params || typeof params !== "object" || Array.isArray(params)) {
        return "Backend-defined";
    }

    const entries = Object.entries(params);
    if (!entries.length) {
        return "Backend-defined";
    }

    return entries
        .map(([key, value]) => `${key}=${formatTableValue(value)}`)
        .join(", ");
}

function buildTrainingComparisonRows(modelResults) {
    return sortTrainingModelResults(modelResults).map((result, index) => ({
        Rank: index + 1,
        "Model Name": result.strModelName,
        Accuracy: formatSummaryValue("fltAccuracy", Number(result.objMetrics?.fltAccuracy) || 0),
        Precision: formatSummaryValue("fltPrecision", Number(result.objMetrics?.fltPrecision) || 0),
        Recall: formatSummaryValue("fltRecall", Number(result.objMetrics?.fltRecall) || 0),
        F1: formatSummaryValue("fltF1", Number(result.objMetrics?.fltF1) || 0),
        "Training Speed": formatRuntimeSeconds(result.fltTimeTaken),
    }));
}

function buildTrainingFeatureRows(modelResults) {
    return sortTrainingModelResults(modelResults).flatMap((result) =>
        normalizeFeatureImportanceRows(result.tblFeatureImportance).map((row) => ({
            Model: result.strModelName,
            Rank: firstDefinedValue(row, ["intRank", "Rank"]) || "",
            Feature: firstDefinedValue(row, ["strFeatureName", "Feature", "FeatureName"]) || "",
            Importance: Number(firstDefinedValue(row, ["fltImportance", "Importance", "Score"])) || 0,
        }))
    );
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
        fragments.push(`Runtime ${formatRuntimeSeconds(timeTaken)}`);
    }
    if (dateCreated) {
        fragments.push(`Logged ${formatDateTime(dateCreated)}`);
    }

    return fragments.join(" | ");
}

function formatRuntimeSeconds(value) {
    const parsedValue = Number(value);
    if (!Number.isFinite(parsedValue) || parsedValue < 0) {
        return "N/A";
    }

    if (parsedValue >= 60) {
        const minutes = Math.floor(parsedValue / 60);
        const seconds = parsedValue % 60;
        return `${minutes}m ${seconds.toFixed(2)}s`;
    }

    return `${parsedValue.toFixed(2)}s`;
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

function setTrainingResultsButtonTone(isReady) {
    const button = document.getElementById("btn-view-training-results");
    if (!button) {
        return;
    }

    button.classList.toggle("primary-button", Boolean(isReady));
    button.classList.toggle("secondary-button", !isReady);
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

function getSelectedTrainingModelIds() {
    return TRAINING_MODEL_OPTIONS
        .filter((option) => document.getElementById(option.inputId)?.checked)
        .map((option) => option.id);
}

function getSelectedTrainingModelLabels() {
    return TRAINING_MODEL_OPTIONS
        .filter((option) => document.getElementById(option.inputId)?.checked)
        .map((option) => option.label);
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
            UI_STATE.trainingColumnInclusion = {};
            UI_STATE.trainingSettingsConfirmed = false;
            renderTrainingDatasetProfile(rows);
            renderTrainingRunShowcase([]);
            renderRunStepChampionDetails(null);
            renderRunStepChampionHyperparameters(null);
            setButtonEnabled("btn-view-training-results", false);
            setTrainingResultsButtonTone(false);
            refreshTrainingSettingsStatus();

            const timeDetails = document.getElementById("time-details");
            if (timeDetails) {
                timeDetails.textContent = "No completed training run yet.";
            }
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
    const profile = buildDatasetProfile(UI_STATE.trainingRows);
    const selectedModels = getSelectedTrainingModelIds();
    const selectedColumns = profile
        ? profile.reviewRows
            .filter(
                (row) =>
                    row.includeInTraining && row.columnName !== profile.targetColumn
            )
            .map((row) => row.columnName)
        : [];

    if (!selectedModels.length) {
        throw new Error("Select at least one model family before starting training.");
    }

    return {
        intRandomState: toNumber("train-random-state", 0),
        fltTTSplit: toNumber("train-train-test-split", 0.7),
        intCrossFold: toNumber("train-cross-fold", 5),
        intPrimaryMetric: toNumber("train-primary-metric", 1),
        intTopFeats: toNumber("train-top-feats", 20),
        fltF1: 1,
        lisintModels: selectedModels,
        lisstrFeats: selectedColumns,
        strFeatTarget: profile?.targetColumn || "",
    };
}

async function trainModel() {
    let requestBody;
    try {
        requestBody = getTrainingRequestBody();
    } catch (error) {
        setStatusMessage(
            "train-upload-status",
            error.message || "Unable to start training.",
            "danger"
        );
        showAppNotice(error.message || "Unable to start training.", "danger");
        return;
    }

    const progress = startProgress(
        "loading-spinner-training-details-preview",
        "progress-bar-container-train",
        "progress-bar-train",
        "progress-label-train"
    );

    const selectedModelLabel =
        requestBody.lisintModels.length === 1
            ? "selected model family"
            : "selected model families";
    setStatusMessage("train-upload-status", "Training selected model families...", "neutral");
    showAppNotice(
        `Training ${requestBody.lisintModels.length} ${selectedModelLabel}...`,
        "neutral"
    );

    try {
        const data = await fetchJson(
            "/train/model",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify(requestBody),
            },
            "Model training failed."
        );

        stopProgress(progress, true);
        UI_STATE.lastTrainingResponse = data;

        try {
            await loadModelHistory(true);
        } catch (historyError) {
            console.error("Model history refresh error:", historyError);
        }

        renderTrainingRunOutputs(data);
        setButtonEnabled("btn-proceed-inference", true);
        setButtonEnabled("btn-view-training-results", true);
        setTrainingResultsButtonTone(true);
        refreshTrainingSettingsStatus();
        setStatusMessage(
            "train-upload-status",
            "Training finished. The scoring workspace is now unlocked.",
            "success"
        );
        setStageStatus("stage-train-status", "Model trained", "success");
        setStageStatus("stage-score-status", "Ready for scoring", "neutral");
        showModelLabStep("run");
        showAppNotice(
            "Training finished. Review the champion board, then move to results or scoring.",
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

function renderTrainingRunOutputs(data) {
    const timeDetails = document.getElementById("time-details");
    const featureImportancePreview = document.getElementById("feature-importance-preview");
    const modelResults = normalizeTrainingModelResults(data.tblModelResults);
    const comparisonRows = buildTrainingComparisonRows(modelResults);
    const featureRows = buildTrainingFeatureRows(modelResults);
    const bestModelName = data.strBestModelName || "Latest run";
    const metadataLine = buildMetadataLine(data.timeTaken, data.dateCreated);

    renderTrainingRunShowcase(modelResults);
    renderRunStepChampionDetails(data);
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

    if (timeDetails) {
        timeDetails.textContent = metadataLine
            ? `${metadataLine} Champion: ${bestModelName}.`
            : `Training completed. Champion: ${bestModelName}.`;
    }

    if (comparisonRows.length > 0) {
        displayTable(
            comparisonRows,
            "historical-models-preview",
            "No model comparison output available."
        );
    }

    if (featureRows.length > 0) {
        displayTable(
            featureRows,
            "feature-importance-preview",
            "No feature importance output available."
        );
    } else if (featureImportancePreview) {
        featureImportancePreview.innerHTML =
            '<p class="empty-state">The current backend training response does not provide feature importance yet.</p>';
    }

    renderTrainingDatasetProfile(UI_STATE.trainingRows, data.objDatasetSplit);
    renderRunStepChampionHyperparameters(data);
    renderLatestTrainingResultSummary(data);
}

function renderTrainingDatasetProfile(rows, datasetSplit = null) {
    const records = normalizeTableData(rows);
    const profile = buildDatasetProfile(records);
    const summaryContainer = document.getElementById("train-dataset-summary");

    renderRunStepTrainingOverview(profile, datasetSplit);

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
        renderTrainingTargetSelector([], "");
        renderTrainingColumnReview([]);
        renderLoadStepReadiness(null);
        renderTrainingFeatureTable([]);
        return;
    }

    renderTrainingTargetSelector(profile.availableColumns, profile.targetColumn);
    renderTrainingColumnReview(profile.reviewRows);
    renderLoadStepReadiness(profile);
    renderTrainingFeatureTable(profile.fields);

    const classSplitPreview = buildTargetClassSplitSummary(records, profile.targetColumn);

    if (classSplitPreview) {
        summaryContainer.appendChild(buildTargetClassSplitCard(classSplitPreview));
    } else {
        summaryContainer.appendChild(
            buildSummaryCard(
                "Target class split",
                "Unavailable",
                "Choose a target feature in Step 2 to inspect its class distribution."
            )
        );
    }

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
            profile.targetIncluded ? humanizeLabel(profile.targetColumn) : "Not selected",
            profile.targetIncluded
                ? "Selected in Step 2 as the target feature for the column review."
                : "Choose a target feature in Step 2 to label it in the review panes."
        )
    );
}

function renderRunStepTrainingOverview(profile, datasetSplit = null) {
    const container = document.getElementById("model-lab-run-training-overview-slot");
    if (!container) {
        return;
    }

    void datasetSplit;
    container.innerHTML = "";

    const randomState = document.getElementById("train-random-state")?.value?.trim() || "None";
    const splitRatio = Math.min(Math.max(toNumber("train-train-test-split", 0.7), 0), 1);
    const crossFold = document.getElementById("train-cross-fold")?.value?.trim() || "None";
    const primaryMetric =
        document
            .getElementById("train-primary-metric")
            ?.selectedOptions?.[0]
            ?.textContent?.trim() || "None";
    const topFeatures = document.getElementById("train-top-feats")?.value?.trim() || "None";

    let trainTestValue = "None";
    let trainTestDescription = "Defaults to none until training rows are available.";

    if (profile && Number.isFinite(profile.rowCount) && profile.rowCount > 0) {
        const projectedTrainRows = Math.round(profile.rowCount * splitRatio);
        const projectedTestRows = Math.max(profile.rowCount - projectedTrainRows, 0);
        trainTestValue = `${projectedTrainRows.toLocaleString()} / ${projectedTestRows.toLocaleString()}`;
        trainTestDescription = "Projected from the current train/test split in General Setting.";
    }

    [
        {
            label: "Random State",
            value: randomState,
            description: "Current seed that will be sent with the training request.",
        },
        {
            label: "Train / Test Split Ratio",
            value: `${splitRatio.toFixed(2)} train / ${(1 - splitRatio).toFixed(2)} test`,
            description: "Current ratio staged in Step 1: General Setting.",
        },
        {
            label: "Train / Test Split Count",
            value: trainTestValue,
            description: trainTestDescription,
        },
        {
            label: "Cross Validation",
            value: crossFold,
            description: "Current cross-fold value from the training configuration.",
        },
        {
            label: "Scoring Metric",
            value: primaryMetric,
            description: "Current backend ranking metric for model comparison.",
        },
        {
            label: "Top Feat Count Scoring",
            value: topFeatures,
            description: "Current feature-importance cutoff in the training request.",
        },
    ].forEach((card) => {
        container.appendChild(buildSummaryCard(card.label, card.value, card.description));
    });
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

function renderTrainingTargetSelector(columns, selectedTarget) {
    const select = document.getElementById("train-target-column"); 
    const note = document.getElementById("train-target-column-note");
    const options = normalizeTableData(columns);

    if (!select) {
        return;
    }

    select.innerHTML = "";

    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Select target column";
    select.appendChild(placeholder);

    options.forEach((columnName) => {
        const option = document.createElement("option");
        option.value = columnName;
        option.textContent = humanizeLabel(columnName);
        select.appendChild(option);
    });

    select.disabled = options.length === 0;
    select.value = selectedTarget && options.includes(selectedTarget) ? selectedTarget : "";

    if (!note) {
        return;
    }

    if (!options.length) {
        note.textContent =
            "Upload a training CSV to choose which feature is the target.";
        return;
    }

    note.textContent =
        "Choose the target feature name here so Step 2 can label it in the review table.";
}

function renderTrainingColumnReview(rows) {
    const container = document.getElementById("model-lab-column-review-preview");
    const reviewRows = normalizeTableData(rows);

    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (!reviewRows.length) {
        container.innerHTML =
            '<p class="empty-state">Upload a training CSV to generate the prescribed column review.</p>';
        return;
    }

    const tableScroll = document.createElement("div");
    tableScroll.className = "table-scroll column-review-scroll";

    const table = document.createElement("table");
    table.className = "column-review-table";
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    ["Feature Name", "Status", "Why", "Include In Training?"].forEach((label) => {
        const th = document.createElement("th");
        th.textContent = label;
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    const tbody = document.createElement("tbody");
    reviewRows.forEach((row) => {
        const tr = document.createElement("tr");

        const featureCell = document.createElement("td");
        featureCell.textContent = row.featureName;

        const statusCell = document.createElement("td");
        statusCell.innerHTML = `
            <span class="column-review-status is-${escapeHtml(row.tone)}">
                ${escapeHtml(row.status)}
            </span>
        `;

        const whyCell = document.createElement("td");
        whyCell.textContent = row.why;

        const includeCell = document.createElement("td");
        includeCell.className = "column-review-include"; 
        const includeInput = document.createElement("input");
        includeInput.type = "checkbox";
        includeInput.checked = Boolean(row.includeInTraining);
        includeInput.disabled = Boolean(row.isLocked);
        includeInput.setAttribute(
            "aria-label",
            `Include ${row.featureName} in training`
        );
        includeInput.addEventListener("change", (event) => {
            handleTrainingIncludeToggle(row.columnName, event.target.checked);
        });
        includeCell.appendChild(includeInput);

        tr.append(featureCell, statusCell, whyCell, includeCell);
        tbody.appendChild(tr);
    });

    table.appendChild(tbody);
    tableScroll.appendChild(table);
    container.appendChild(tableScroll);

    requestAnimationFrame(() => {
        lockTableViewport(
            tableScroll,
            table,
            Math.min(COLUMN_REVIEW_VISIBLE_ROWS, reviewRows.length)
        );
    });
}

function handleTrainingTargetChange(value) {
    const previousTarget = UI_STATE.trainingTargetColumn;
    if (previousTarget && previousTarget !== value) {
        delete UI_STATE.trainingColumnInclusion[previousTarget];
    }

    UI_STATE.trainingTargetColumn = value || null;
    renderTrainingDatasetProfile(
        UI_STATE.trainingRows,
        UI_STATE.lastTrainingResponse?.objDatasetSplit || null
    );
    refreshTrainingSettingsStatus();
}

function handleTrainingIncludeToggle(columnName, includeInTraining) {
    if (!columnName) {
        return;
    }

    if (UI_STATE.trainingTargetColumn && columnName === UI_STATE.trainingTargetColumn) {
        UI_STATE.trainingColumnInclusion[columnName] = true;
    } else {
        UI_STATE.trainingColumnInclusion[columnName] = Boolean(includeInTraining);
    }

    renderTrainingDatasetProfile(
        UI_STATE.trainingRows,
        UI_STATE.lastTrainingResponse?.objDatasetSplit || null
    );
    refreshTrainingSettingsStatus();
}

function renderLoadStepReadiness(profile) {
    const container = document.getElementById("model-lab-load-next-summary");
    const hasProfile = Boolean(profile);

    setButtonEnabled("btn-proceed-model-run", hasProfile);

    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (!hasProfile) {
        container.appendChild(
            buildSummaryCard(
                "Training step",
                "Awaiting upload",
                "Load a training CSV first to unlock the run step."
            )
        );
        return;
    }

    const includedCount = profile.reviewRows.filter((row) => row.includeInTraining).length;
    const excludedCount = profile.reviewRows.length - includedCount;

    container.appendChild(
        buildSummaryCard(
            "Dataset",
            `${profile.rowCount.toLocaleString()} row(s)`
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Target feature",
            profile.targetIncluded ? humanizeLabel(profile.targetColumn) : "Not selected",
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Column review",
            `${includedCount.toLocaleString()} keep / ${excludedCount.toLocaleString()} exclude`,
        )
    );
}

function buildDatasetProfile(rows) {
    const records = normalizeTableData(rows);
    if (!records.length) {
        return null;
    }

    const columns = Object.keys(records[0]);
    const targetColumn = resolveTrainingTargetColumn(columns);
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
            Role: columnName === targetColumn ? "Target" : "Feature",
            Type: columnType,
            Filled: `${filledPercent}%`,
            UniqueValues: uniqueCount.toLocaleString(),
            Sample: formatTableValue(sample),
        };
    });

    const numericCount = fieldRows.filter((row) => row.Type !== "Categorical").length;
    const reviewRows = columns.map((columnName) =>
        buildTrainingColumnReviewRow(
            columnName,
            records
                .map((row) => row[columnName])
                .filter((value) => value !== null && value !== undefined && value !== ""),
            records.length,
            targetColumn
        )
    );

    return {
        rowCount: records.length,
        fieldCount: columns.length,
        numericCount,
        categoricalCount: columns.length - numericCount,
        targetIncluded: Boolean(targetColumn && columns.includes(targetColumn)),
        targetColumn,
        availableColumns: columns,
        fields: fieldRows,
        reviewRows,
    };
}

function buildTargetClassSplitSummary(rows, targetColumn) {
    const records = normalizeTableData(rows);

    if (!records.length || !targetColumn) {
        return null;
    }

    const classCounts = new Map();
    let missingCount = 0;

    records.forEach((row) => {
        const value = row[targetColumn];
        if (value === null || value === undefined || value === "") {
            missingCount += 1;
            return;
        }

        const label = formatTableValue(value);
        classCounts.set(label, (classCounts.get(label) || 0) + 1);
    });

    if (!classCounts.size) {
        return null;
    }

    const sortedClassCounts = [...classCounts.entries()].sort((left, right) => {
        const countDelta = right[1] - left[1];
        if (countDelta !== 0) {
            return countDelta;
        }
        return left[0].localeCompare(right[0]);
    });

    return {
        targetLabel: humanizeLabel(targetColumn),
        populatedCount: [...classCounts.values()].reduce((total, count) => total + count, 0),
        missingCount,
        classCount: sortedClassCounts.length,
        rows: sortedClassCounts.map(([label, count]) => ({
            label,
            count,
        })),
    };
}

function buildTargetClassSplitCard(summary) {
    const card = document.createElement("div");
    card.className = "summary-card summary-card-class-split";

    const kicker = document.createElement("span");
    kicker.className = "kicker";
    kicker.textContent = "Target class split";

    const title = document.createElement("strong");
    title.textContent = summary.targetLabel;

    const description = document.createElement("p");
    description.textContent = `${summary.classCount.toLocaleString()} class(es) across ${summary.populatedCount.toLocaleString()} populated row(s).`;

    const list = document.createElement("div");
    list.className = "class-split-list";

    summary.rows.slice(0, 4).forEach((row) => {
        const percent = summary.populatedCount > 0 ? row.count / summary.populatedCount : 0;
        const rowElement = document.createElement("div");
        rowElement.className = "class-split-row";
        rowElement.innerHTML = `
            <div class="class-split-row-top">
                <span class="class-split-label">${escapeHtml(String(row.label))}</span>
                <span class="class-split-stat">${row.count.toLocaleString()} row(s) | ${formatPercentage(percent)}</span>
            </div>
            <div class="class-split-bar">
                <span style="width:${Math.max(percent * 100, percent > 0 ? 8 : 0).toFixed(1)}%"></span>
            </div>
        `;
        list.appendChild(rowElement);
    });

    card.append(kicker, title, description, list);

    if (summary.classCount > 4) {
        const moreNote = document.createElement("p");
        moreNote.className = "class-split-note";
        moreNote.textContent = `+${(summary.classCount - 4).toLocaleString()} more class(es) not shown in this preview.`;
        card.appendChild(moreNote);
    }

    if (summary.missingCount > 0) {
        const missingNote = document.createElement("p");
        missingNote.className = "class-split-note";
        missingNote.textContent = `${summary.missingCount.toLocaleString()} row(s) are blank for this target.`;
        card.appendChild(missingNote);
    }

    return card;
}

function resolveTrainingTargetColumn(columns) {
    const availableColumns = normalizeTableData(columns).filter((columnName) =>
        typeof columnName === "string" && columnName.length > 0
    );

    if (!availableColumns.length) {
        UI_STATE.trainingTargetColumn = null;
        return null;
    }

    if (
        UI_STATE.trainingTargetColumn &&
        availableColumns.includes(UI_STATE.trainingTargetColumn)
    ) {
        return UI_STATE.trainingTargetColumn;
    }

    if (availableColumns.includes("Exited")) {
        UI_STATE.trainingTargetColumn = "Exited";
        return UI_STATE.trainingTargetColumn;
    }

    UI_STATE.trainingTargetColumn = null;
    return null;
}

function buildTrainingColumnReviewRow(columnName, values, rowCount, targetColumn = null) {
    const normalizedName = String(columnName || "").toLowerCase();
    const normalizedTarget = String(targetColumn || "").toLowerCase();
    const uniqueCount = new Set(values.map((value) => String(value))).size;
    const fillRate = rowCount > 0 ? values.length / rowCount : 0;
    const columnType = detectColumnType(values);
    const isTargetColumn = Boolean(normalizedTarget && normalizedName === normalizedTarget);

    let status = "OK";
    let why = "Column is suitable for the current training workflow.";
    let includeInTraining = true;
    let tone = "success";

    if (isTargetColumn) {
        status = "Target";
        why = "Chosen in Step 2 as the target feature.";
        includeInTraining = true;
        tone = "success";
    } else if (["customerid", "rownumber", "surname", "email"].includes(normalizedName)) {
        status = "Drop";
        why = "Identifier or personal field with weak modelling value.";
        includeInTraining = false;
        tone = "danger";
    } else if (values.length === 0) {
        status = "Review";
        why = "Column is empty in the uploaded dataset preview.";
        includeInTraining = false;
        tone = "warning";
    } else if (
        columnType === "Categorical" &&
        uniqueCount >= Math.max(50, Math.round(rowCount * 0.6))
    ) {
        status = "Drop";
        why = "High-cardinality categorical values are likely identifier-like.";
        includeInTraining = false;
        tone = "danger";
    } else if (fillRate < 0.6) {
        status = "Review";
        why = "Lower fill rate than the rest of the staged dataset.";
        includeInTraining = true;
        tone = "warning";
    }

    includeInTraining = resolveTrainingColumnInclusion(
        columnName,
        includeInTraining,
        targetColumn
    );

    return {
        columnName,
        featureName: humanizeLabel(columnName),
        status,
        why,
        includeInTraining,
        tone,
        isLocked: isTargetColumn,
    };
}

function resolveTrainingColumnInclusion(
    columnName,
    defaultIncludeInTraining,
    targetColumn = null
) {
    if (targetColumn && columnName === targetColumn) {
        return true;
    }

    if (Object.prototype.hasOwnProperty.call(UI_STATE.trainingColumnInclusion, columnName)) {
        return Boolean(UI_STATE.trainingColumnInclusion[columnName]);
    }

    return Boolean(defaultIncludeInTraining);
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
    renderTrainingRunShowcase([]);
    renderRunStepChampionDetails(null);
    renderRunStepChampionHyperparameters(null);
    setButtonEnabled("btn-view-training-results", false);
    setTrainingResultsButtonTone(false);
    refreshTrainingSettingsStatus();
    renderBestModelPlaceholder(
        "Run training or load historical model data to populate this view."
    );
    renderHistoricalModelsPlaceholder(
        "No additional historical models are available yet."
    );
}

function renderRunStepChampionDetails(data) {
    const container = document.getElementById("model-lab-run-champion-slot");
    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (!data) {
        container.appendChild(
            buildSummaryCard(
                "Champion",
                "Pending run",
                "Train the selected model families to populate the winning model details."
            )
        );
        return;
    }

    const metrics = normalizeTableData(data.objMetrics)[0] || {};
    const modelResults = normalizeTrainingModelResults(data.tblModelResults);
    const championResult = modelResults.find((row) => row.boolIsChampion) || modelResults[0] || null;
    const championRuntime = championResult ? championResult.fltTimeTaken : data.timeTaken;

    container.appendChild(
        buildSummaryCard(
            "Champion",
            data.strBestModelName || "Latest run",
            modelResults.length
                ? `Selected from ${modelResults.length} trained candidate models.`
                : "Directly returned by the training endpoint."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Training Speed",
            formatRuntimeSeconds(championRuntime),
            "Elapsed time for the winning model candidate."
        )
    );
    container.appendChild(
        buildSummaryCard("F1", formatSummaryValue("fltF1", metrics.fltF1), "Primary score from the latest run.")
    );
    container.appendChild(
        buildSummaryCard(
            "Accuracy",
            formatSummaryValue("fltAccuracy", metrics.fltAccuracy),
            "Overall accuracy from the winning model."
        )
    );
    container.appendChild(
        buildSummaryCard(
            "Precision / Recall",
            `${formatSummaryValue("fltPrecision", metrics.fltPrecision)} / ${formatSummaryValue("fltRecall", metrics.fltRecall)}`,
            "Positive class precision and recall for the current champion."
        )
    );
}

function renderRunStepChampionHyperparameters(data) {
    const container = document.getElementById("model-lab-run-hyperparams-slot");
    if (!container) {
        return;
    }

    container.innerHTML = "";

    if (!data) {
        container.innerHTML =
            '<p class="empty-state">Train the selected model families to reveal the champion hyperparameters.</p>';
        return;
    }

    const modelResults = normalizeTrainingModelResults(data.tblModelResults);
    const championResult = modelResults.find((row) => row.boolIsChampion) || modelResults[0] || null;
    const championParams =
        championResult?.dicBestParams && typeof championResult.dicBestParams === "object"
            ? Object.entries(championResult.dicBestParams)
            : [];

    if (!championParams.length) {
        container.innerHTML =
            '<p class="empty-state">No champion hyperparameters were returned by the backend for this run.</p>';
        return;
    }

    championParams.forEach(([key, value]) => {
        const row = document.createElement("div");
        row.className = "model-run-param-row";
        row.innerHTML = `
            <span class="model-run-param-key">${escapeHtml(humanizeLabel(key))}</span>
            <strong class="model-run-param-value">${escapeHtml(formatTableValue(value))}</strong>
        `;
        container.appendChild(row);
    });
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
    const bestModelName = data.strBestModelName || "Latest run";
    const modelResults = normalizeTrainingModelResults(data.tblModelResults);
    container.appendChild(
        buildSummaryCard(
            "Champion",
            bestModelName,
            modelResults.length
                ? `Selected from ${modelResults.length} trained candidate models.`
                : "Directly returned by the training endpoint."
        )
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
    return sortModelRecords(rows)[0];
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

    if (!normalizeFeatureImportanceRows(UI_STATE.lastTrainingResponse?.tblFeatureImportance).length) {
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

function renderTrainingRunShowcase(modelResults) {
    const container = document.getElementById("model-lab-run-leaderboard");
    if (!container) {
        return;
    }

    const comparisonRows = buildTrainingComparisonRows(modelResults);
    if (!comparisonRows.length) {
        container.innerHTML =
            '<p class="empty-state">Select the model families in General Setting, then start training to compare runtime and metrics here.</p>';
        return;
    }

    displayTable(
        comparisonRows,
        "model-lab-run-leaderboard",
        "No model comparison output available."
    );
}

function buildLiveTrainingParamsLabel() {
    const params = [];
    const randomState = document.getElementById("train-random-state")?.value;
    const splitRatio = document.getElementById("train-train-test-split")?.value;
    const crossFold = document.getElementById("train-cross-fold")?.value;
    const primaryMetric = document.getElementById("train-primary-metric");
    const topFeatures = document.getElementById("train-top-feats")?.value;
    const selectedModels = getSelectedTrainingModelLabels();
    const primaryMetricLabel = primaryMetric?.selectedOptions?.[0]?.textContent?.trim();

    if (randomState) {
        params.push(`Random=${randomState}`);
    }
    if (splitRatio) {
        params.push(`Split=${splitRatio}`);
    }
    if (crossFold) {
        params.push(`CV=${crossFold}`);
    }
    if (primaryMetricLabel) {
        params.push(`Metric=${primaryMetricLabel}`);
    }
    if (topFeatures) {
        params.push(`TopFeats=${topFeatures}`);
    }
    if (selectedModels.length) {
        params.push(`Models=${selectedModels.join(", ")}`);
    }

    return params.join(" | ") || "Current backend pipeline";
}

function formatLeaderboardMetricValue(key, value) {
    if (typeof value === "string" && value.trim() !== "" && !Number.isNaN(Number(value))) {
        return formatSummaryValue(key, Number(value));
    }

    return formatSummaryValue(key, value);
}

function resolveLeaderboardModelName(row) {
    return (
        firstDefinedValue(row, [
            "Model",
            "ModelName",
            "Algorithm",
            "ModelType",
            "ModelFamily",
            "Estimator",
            "Name",
        ]) || "Current backend pipeline"
    );
}

function resolveLeaderboardModelParams(row) {
    const value = firstDefinedValue(row, [
        "ModelParams",
        "Params",
        "ModelParameters",
        "Hyperparameters",
        "HyperParameters",
        "Parameters",
    ]);

    if (value === null || value === undefined || value === "") {
        return "Backend-defined";
    }

    if (typeof value === "object") {
        return JSON.stringify(value);
    }

    return String(value);
}

function firstDefinedValue(row, keys) {
    for (const key of keys) {
        if (row && row[key] !== undefined && row[key] !== null && row[key] !== "") {
            return row[key];
        }
    }
    return null;
}

function sortModelRecords(rows) {
    return [...normalizeTableData(rows)].sort((left, right) => {
        const f1Delta =
            (Number(firstDefinedValue(right, ["F1", "fltF1"])) || 0) -
            (Number(firstDefinedValue(left, ["F1", "fltF1"])) || 0);
        if (f1Delta !== 0) {
            return f1Delta;
        }

        const accuracyDelta =
            (Number(firstDefinedValue(right, ["Accuracy", "fltAccuracy"])) || 0) -
            (Number(firstDefinedValue(left, ["Accuracy", "fltAccuracy"])) || 0);
        if (accuracyDelta !== 0) {
            return accuracyDelta;
        }

        return new Date(right.meta_DateCreated || 0) - new Date(left.meta_DateCreated || 0);
    });
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

function buildSummaryCard(label, value, description = null) {
    const card = document.createElement("div");
    card.className = "summary-card";
    card.innerHTML = `
        <span class="kicker">${escapeHtml(label)}</span>
        <strong>${escapeHtml(String(value))}</strong>
    `;

    if (description){
        card.innerHTML += `<p>${escapeHtml(description)}</p>`;
    }

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
    requestAnimationFrame(() => {
        lockTableViewport(tableScroll, table, TABLE_VISIBLE_ROWS);
    });
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

    if (headerHeight === 0 && rowsHeight === 0) {
        tableScroll.style.removeProperty("height");
        tableScroll.style.removeProperty("max-height");
        return;
    }

    const viewportHeight = headerHeight + rowsHeight + 2;
    tableScroll.style.height = `${viewportHeight}px`;
    tableScroll.style.maxHeight = `${viewportHeight}px`;
}

function refreshTableViewports() {
    document.querySelectorAll(".table-scroll").forEach((tableScroll) => {
        const table = tableScroll.querySelector("table");
        if (!table) {
            return;
        }

        lockTableViewport(tableScroll, table, TABLE_VISIBLE_ROWS);
    });
}

function downloadRowsAsCsv(rows, filename) {
    if (!rows.length) {
        return;
    }

    const header = Object.keys(rows[0]);
    const csvRows = [header.join(",")];

    rows.forEach((row) => {
        const values = header.map((key) =>
            `"${formatTableValue(row[key]).replace(/"/g, '""')}"`
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

    if (typeof value === "object") {
        return JSON.stringify(value);
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

    if (typeof value === "object") {
        return JSON.stringify(value);
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
