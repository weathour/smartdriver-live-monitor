# Product

## Register

product

## Users

SmartDriver developers and vehicle-test engineers using a Windows/Linux computer or an Android phone beside the Unity HMI. They need to confirm, during a short field-test window, exactly what Redis and UDP data reached the current application.

## Product Purpose

Receive the same external Redis keys and UDP port used by SmartDriver, show changes live without rendering the driving scene, and optionally preserve every observation and raw payload. Success means the operator can distinguish no data, unchanged data, malformed data, and changing data.

## Brand Personality

Calm, precise, diagnostic. Labels state measured facts and never imply that a parsed or repeated payload is newer than it is.

## Anti-references

Avoid decorative automotive dashboards, gauges, animated vehicle scenes, hidden filters, color-only status, and oversimplified healthy/unhealthy labels.

## Design Principles

1. Put evidence before interpretation.
2. Keep receiving, live display, and recording separate.
3. Show last observation separately from last content change.
4. Keep parse errors and stale values visible instead of suppressing them.
5. Use familiar native controls on Windows, Linux, and Android, with a dependency-free desktop headless mode for unattended recording.
6. Keep Android foreground-only and read-only; do not imply its bounded UI history is a recording.

## Accessibility

Use readable system fonts, keyboard-reachable or touch-reachable controls, and text labels in addition to color. Do not rely on essential animation. The interface targets Chinese Windows/Linux desktops and Android phones or tablets.
