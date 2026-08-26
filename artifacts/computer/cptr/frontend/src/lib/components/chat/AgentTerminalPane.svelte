<script lang="ts">
	import Terminal from '$lib/components/Terminal.svelte';
	import Icon from '$lib/components/Icon.svelte';

	interface Props {
		runId: string;
		status?: string;
		wsPath: string;
		initialOutput?: string;
		oncancel?: () => void;
	}

	let { runId, status = 'running', wsPath, initialOutput = '', oncancel }: Props = $props();
	let collapsed = $state(false);

	const normalizedStatus = $derived(String(status || 'running').toLowerCase().replaceAll('_', ' '));
	const active = $derived(!['succeeded', 'completed', 'failed', 'cancelled', 'manual review required'].includes(normalizedStatus));
</script>

<section class="agent-terminal-pane" data-testid="agent-terminal-pane" aria-label="Heidi live terminal">
	<header class="agent-terminal-header">
		<div class="agent-terminal-title">
			<span class:active class="terminal-status" aria-hidden="true"></span>
			<div class="min-w-0">
				<div class="agent-terminal-name">Heidi terminal</div>
				<div class="agent-terminal-meta">
					<span>{normalizedStatus}</span>
					<span aria-hidden="true">·</span>
					<span title={runId}>run {runId.slice(0, 10)}</span>
					<span aria-hidden="true">·</span>
					<span>read-only mirror</span>
				</div>
			</div>
		</div>
		<div class="agent-terminal-actions">
			{#if active && oncancel}
				<button type="button" class="terminal-cancel" onclick={() => oncancel?.()}>
					<Icon name="cancel" size={13} />
					Cancel run
				</button>
			{/if}
			<button
				type="button"
				class="terminal-action"
				onclick={() => (collapsed = !collapsed)}
				aria-expanded={!collapsed}
			>
				{collapsed ? 'Expand' : 'Collapse'}
			</button>
		</div>
	</header>

	{#if !collapsed}
		<div class="agent-terminal-body">
			<Terminal
				wsPath={wsPath}
				initialOutput={initialOutput}
				readOnly={true}
				observerMode={true}
			/>
		</div>
	{:else}
		<div class="agent-terminal-collapsed">Live terminal output is collapsed.</div>
	{/if}
</section>

<style>
	.agent-terminal-pane {
		margin: 0 0 0.75rem;
		overflow: hidden;
		border: 1px solid color-mix(in oklab, var(--app-fg) 13%, transparent);
		border-radius: 0.875rem;
		background: color-mix(in oklab, var(--app-bg) 94%, var(--app-fg));
		box-shadow: 0 16px 42px color-mix(in oklab, #020617 20%, transparent);
	}
	.agent-terminal-header {
		display: flex;
		align-items: center;
		justify-content: space-between;
		gap: 0.75rem;
		min-height: 2.85rem;
		padding: 0.55rem 0.75rem;
		border-bottom: 1px solid color-mix(in oklab, var(--app-fg) 10%, transparent);
	}
	.agent-terminal-title,
	.agent-terminal-actions { display: flex; align-items: center; gap: 0.5rem; min-width: 0; }
	.terminal-status { width: 0.52rem; height: 0.52rem; flex: 0 0 auto; border-radius: 999px; background: #64748b; }
	.terminal-status.active { background: #34d399; box-shadow: 0 0 0 4px color-mix(in oklab, #34d399 13%, transparent); animation: terminal-pulse 1.7s ease-in-out infinite; }
	.agent-terminal-name { color: var(--app-fg); font: 650 0.69rem ui-sans-serif, system-ui, sans-serif; letter-spacing: 0.07em; text-transform: uppercase; }
	.agent-terminal-meta { display: flex; min-width: 0; gap: 0.32rem; overflow: hidden; color: color-mix(in oklab, var(--app-fg) 56%, transparent); font: 0.6rem ui-monospace, SFMono-Regular, monospace; text-transform: capitalize; white-space: nowrap; }
	.terminal-action,
	.terminal-cancel { border: 1px solid color-mix(in oklab, var(--app-fg) 14%, transparent); border-radius: 0.45rem; padding: 0.3rem 0.5rem; color: color-mix(in oklab, var(--app-fg) 70%, transparent); font: 650 0.63rem ui-sans-serif, system-ui, sans-serif; }
	.terminal-action:hover { background: color-mix(in oklab, var(--app-fg) 7%, transparent); color: var(--app-fg); }
	.terminal-cancel { display: inline-flex; align-items: center; gap: 0.32rem; border-color: color-mix(in oklab, #fb7185 36%, transparent); color: #fda4af; }
	.terminal-cancel:hover { background: color-mix(in oklab, #be123c 18%, transparent); border-color: #fb7185; }
	.agent-terminal-body { height: min(52vh, 30rem); min-height: 15rem; }
	.agent-terminal-collapsed { padding: 0.75rem; color: color-mix(in oklab, var(--app-fg) 56%, transparent); font: 0.66rem ui-monospace, SFMono-Regular, monospace; }
	@keyframes terminal-pulse { 50% { opacity: 0.5; box-shadow: 0 0 0 7px color-mix(in oklab, #34d399 0%, transparent); } }
	@media (max-width: 640px) {
		.agent-terminal-header { align-items: flex-start; padding: 0.5rem 0.6rem; }
		.agent-terminal-actions { gap: 0.3rem; }
		.agent-terminal-body { height: 42vh; min-height: 13rem; }
		.terminal-cancel { font-size: 0; padding: 0.35rem; }
		.terminal-cancel :global(svg) { width: 0.85rem; height: 0.85rem; }
	}
	@media (prefers-reduced-motion: reduce) { .terminal-status.active { animation: none; } }
</style>
