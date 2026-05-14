/**
 * Hooks Artillery para el load test de Signal Catcher.
 *
 * - setEventContext: escenario “solo impresión” — timestamp + estado + ids aleatorios por request.
 * - initImpressionClickFunnel / initFullFunnel: generan una tanda de IDs coherentes antes del embudo.
 * - funnelTimestamp: actualiza solo el timestamp ISO antes de cada POST del embudo (mismo state/session).
 */
const { randomUUID } = require('crypto');

const STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'WA', 'CO', 'GA', 'OH', 'MI'];

/** Artillery 2 no siempre inicializa `context.vars` antes del primer hook. */
function ensureVars(context) {
  if (!context.vars) {
    context.vars = {};
  }
  return context.vars;
}

function pickState() {
  return STATES[Math.floor(Math.random() * STATES.length)];
}

function setEventContext(context, events, done) {
  const v = ensureVars(context);
  v.timestamp = new Date().toISOString();
  v.state = pickState();
  done();
}

function initImpressionClickFunnel(context, events, done) {
  const v = ensureVars(context);
  v.impId = randomUUID();
  v.clickId = randomUUID();
  v.funnelState = pickState();
  v.sessionId = randomUUID();
  done();
}

function initFullFunnel(context, events, done) {
  const v = ensureVars(context);
  v.impId = randomUUID();
  v.clickId = randomUUID();
  v.convId = randomUUID();
  v.funnelState = pickState();
  v.sessionId = randomUUID();
  done();
}

function funnelTimestamp(context, events, done) {
  const v = ensureVars(context);
  v.timestamp = new Date().toISOString();
  v.state = v.funnelState;
  done();
}

module.exports = {
  setEventContext,
  initImpressionClickFunnel,
  initFullFunnel,
  funnelTimestamp,
};
