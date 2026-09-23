# NYC Project Signals design system

The operator reads projects at a desk in daylight. Use a light research workspace with a dark blue navigation rail, blue-green actions, and a structured evidence pane. System sans typography supports dense addresses, long descriptions and predictable controls.

## Surface

Operate mode: choose a feed, narrow the territory, inspect the evidence, save or export. The first viewport shows real projects beside their source history. Keep dates and source types explicit; never use an invented confidence percentage.

## Direction

Grounded candidates considered: municipal register, contractor job board, property dossier, territory planning sheet, materials specification index, agency research inbox, and a project-control desk. The required direction seed assigned candidate seven: project-control desk. Its persistent sidebar, indexed rows and adjacent evidence sheet fit the approved research workflow. Catalog challengers did not improve either task clarity or audience fit. The approved implementation request delegates routine visual choices; no additional approval flow is introduced.

## Tokens and behavior

- Ink #18282d; muted #53686f; canvas #f3f6f7; white sheet #ffffff.
- Navigation #102e39; action #086d64; pale action #e5f3ef; warning #835b1e on #fff6e5.
- System UI font; 14px table text, 15px body, 30px page heading.
- Flat dividers, restrained 8px field corners, no decorative metrics or illustrations.
- Focus rings, native buttons and selects, explicit loading/error/empty states.
- On smaller screens, collapse navigation into a horizontal feed selector and stack evidence below results.
- On desktop, the results list and evidence pane occupy a bounded workspace and scroll independently; on mobile the workspace returns to natural document flow to avoid nested scroll traps.
- Feed selection uses pressed state, selected evidence is announced through the live detail region, and source links identify their new-tab behavior.
- Saved items are browser-local, clearly labeled; server is a localhost research demo.
