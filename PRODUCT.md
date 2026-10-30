# Product

## Register

product

## Users

SmartDriver developers and vehicle-test engineers using a Windows/Linux computer or an Android phone beside the Unity HMI. During a short field test, desktop operators need to see the latest Redis and UDP values, record the session, and review or transfer it as one file.

## Product Purpose

Receive the external Redis keys and UDP port used by SmartDriver and show current values without rendering the driving scene. Desktop recording keeps Redis observations and bounded UDP snapshots with the selected raw payloads and diagnostic counts. Success means the operator can distinguish no data, unchanged or stale data, malformed data, and changing data, then inspect the recording without managing a collection of files.

## Brand Personality

Calm, precise, diagnostic. Labels state measured facts and never imply that a parsed or repeated payload is newer than it is.

## Anti-references

Avoid decorative automotive dashboards, gauges, animated vehicle scenes, hidden filters, color-only status, and oversimplified healthy/unhealthy labels.

## Design Principles

1. Put evidence before interpretation.
2. Keep receiving, live display, and recording separate; explain that UDP snapshots do not retain every packet.
3. Show last observation separately from last content change.
4. Keep parse errors and stale values visible instead of suppressing them.
5. Show one latest row per stream and keep processing and file viewing bounded; never make an old value appear freshly received.
6. Keep Android foreground-only and read-only; do not imply its bounded UI history is a recording.
7. Use familiar native controls on Windows, Linux, and Android, with a dependency-free desktop headless mode for unattended recording.

## Accessibility

Use readable system fonts, keyboard-reachable or touch-reachable controls, and text labels in addition to color. Do not rely on essential animation. The interface targets Chinese Windows/Linux desktops and Android phones or tablets.
