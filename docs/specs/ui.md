# KubeGraph Ops UI Specification (egui)

This document defines the UI structure, behavior, and visual system for a Kubernetes investigation tool. The implementation target is Rust using immediate mode UI (egui). The reference mock is `gui.jpg`.

---

## 1) App Frame & Layout (Holy Grail)

**Root layout**: `TopBottomPanel::top` (Header), `TopBottomPanel::bottom` (Footer), `SidePanel::left` (Nav), `SidePanel::right` (Inspector), `CentralPanel` (Feed).

### 1.1 Header (persistent)
- Height: 56 px
- Structure (left → right):
  - **Brand**: “KubeGraph Ops” + small icon.
  - **Cluster Context**: “Connected to: production-us-east-1 (K8s v1.28)”
  - **Global Pulse**: nodes/properties counts with sparklines.
- Pulse metrics:
  - `Nodes: 145,203 (↑)`
  - `Properties: 1,204,500`
- Each metric has a tiny sparkline (8–12 points). Render with `egui::Painter::line_segment` or `egui_plot` if available.

### 1.2 Left Nav (persistent)
- Width: 72–96 px
- Icon-only tabs:
  - History
  - Saved
  - Alerts (optional)
- Hover tooltip labels.

### 1.3 Right Inspector (contextual)
- Width: 320–360 px
- Hidden by default; slides in when a node/entity is selected.
- Sections:
  - Header: `NODE_TYPE: node_id`
  - Properties list (key/value)
  - Relationships list (directional)
- Clicking relationship triggers a graph drilldown event (updates selection + pushes a new feed item if configured).

### 1.4 Footer (persistent)
- Height: 64–72 px
- Contains command deck input.
- Prompt marker: `>_` and a wide input field.
- Right-aligned helper icons (commands, history, help).
- Enter triggers a “data packet” animation in the feed (simple: add a transient feed item with a moving indicator or fade-in).

### 1.5 Central Panel (Feed)
- Vertical scroll area. Each interaction is a block:
  1) User question bubble (left-aligned)
  2) System response block (full width)

---

## 2) Core Feed Components

### 2.1 User Question Bubble
- Left aligned, 60–70% width
- Text: natural language query
- Visual: rounded rectangle, low-emphasis border, subtle gradient

### 2.2 System Response Block
Contains three layers (collapsible and dynamic):

#### Layer 1: Generated Cypher (collapsible)
- Accordion label: `GENERATED CYPHER ▸` (closed), `GENERATED CYPHER ▾` (open)
- When open: monospaced editor-like box with syntax highlighting (basic: keyword color + string color)
- Buttons on top-right:
  - `Copy`
  - `Run` (sandbox)

#### Layer 2: Result Visualization (dynamic)
Auto-select renderer based on result type:
- **Metric** (single number): Big Number card
- **List** (entity rows): Entity Card grid
- **Topology** (relationships): Inline graph viz

#### Layer 3: Actions
- Buttons: `👍`, `👎`, `Share Result`, `Pin to Dashboard`
- Small button size (24–28 px height)

---

## 3) Result Renderers

### 3.1 Metric: Big Number Card
- Size: 280–360 px width, 120–160 px height
- Icon on left (e.g., skull for OOMKilled)
- Main text: `14 Pods`
- Subtext: `OOMKilled`
- Color emphasis: warm orange/red

### 3.2 List: Entity Cards
- Grid layout (2–4 columns depending on width)
- Each card shows:
  - Pod name
  - Namespace
  - Status badge
- Click selects entity → opens Inspector

### 3.3 Topology: Inline Graph
- Embedded force-directed sub-graph in the feed
- Controls: zoom/pan within this block
- Nodes are clickable; selecting node updates Inspector
- Minimal node styling: label + icon by type

---

## 4) Visual System

### 4.1 Colors
Define CSS-like tokens for egui theme:
- `bg_primary`: #0B1117
- `bg_panel`: #121A23
- `bg_elevated`: #1B2430
- `accent`: #4CC9F0 (cyan)
- `accent_warm`: #F4A261 (orange)
- `danger`: #E76F51
- `text_primary`: #E6EDF3
- `text_muted`: #8AA0B2
- `border`: #263241

### 4.2 Typography
- Primary: “JetBrains Mono” or “Fira Code” (monospace)
- Sizes:
  - Header: 16–18 px
  - Body: 13–14 px
  - Muted UI: 12 px
  - Big Number: 32–42 px

### 4.3 Effects
- Panel shadows: subtle via darker border + slight gradient
- Glow: only on important metrics (Pulse, Big Number)
- Sparklines: thin line, 1.5 px

---

## 5) Interactions & Behavior

### 5.1 Input & Autocomplete
- Slash commands supported:
  - `/explain pod-123`
  - `/history`
- Autocomplete: show ontology suggestions as user types (`OOMKilledState`, `Pod`, `Container`)
- Suggestion list is filtered by prefix and appears above the input

### 5.2 Feed Events
- New user query inserts a bubble immediately
- System response block is added once translation/graph query completes
- Latency states:
  - “Translating…” (shows skeleton block)
  - “Running Cypher…” (spinner in result area)

### 5.3 Inspector Behavior
- Appears when user clicks entity card or graph node
- Updates on selection change
- Close button `X` in header

---

## 6) Data Model (UI View Models)

### 6.1 FeedItem
```
FeedItem {
  id: String,
  user_text: String,
  cypher: String,
  result: ResultPayload,
  created_at: DateTime,
  state: FeedState
}
```

### 6.2 ResultPayload (enum)
```
ResultPayload::Metric { label, value, unit, icon }
ResultPayload::List { entities: Vec<EntitySummary> }
ResultPayload::Graph { nodes: Vec<Node>, edges: Vec<Edge> }
```

### 6.3 InspectorState
```
InspectorState {
  is_open: bool,
  node_id: Option<String>,
  node_type: Option<String>,
  properties: Vec<(String,String)>,
  relationships: Vec<Relationship>
}
```

---

## 7) egui Implementation Notes

- Use `TopBottomPanel` and `SidePanel` for the Holy Grail layout.
- Feed uses `ScrollArea::vertical().auto_shrink([false;2])`.
- Use `egui::Frame` with custom `fill`, `stroke`, `rounding` for cards.
- Sparklines can be drawn via `ui.painter().line_segment` or `egui_plot` (if enabled).
- Big Number card: custom `Frame` + `ui.vertical_centered` with big font.
- Autocomplete popup: `egui::Area` positioned above footer input.

---

## 8) States & Error Handling

- Empty state: “Ask about your cluster…” in feed center
- Query error: system block shows error banner with retry button
- Graph DB disconnected: header displays `DISCONNECTED` badge

---

## 9) Accessibility & Responsiveness

- Minimum contrast ratio 4.5:1 for text
- Keyboard focus visible on all interactive elements
- Responsive widths:
  - Left nav collapses to 56 px if width < 1100 px
  - Right inspector auto-hides if width < 1000 px

---

## 10) Assets & Icons

- Prefer simple SVG icons (pods, nodes, skull, share, pin).
- Use consistent 16–18 px icon size.

---

## 11) Acceptance Checklist

- Holy Grail layout implemented with persistent header/footer and side panels.
- Feed contains user query + system response blocks.
- Cypher accordion with Copy + Run buttons.
- Result renderer supports Metric, List, Graph.
- Inspector panel shows properties + relationships and reacts to selection.
- Pulse metrics with sparklines visible in header.
- Command deck input with autocomplete and slash command handling.

