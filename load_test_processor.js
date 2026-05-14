/**
 * Hooks Artillery para el load test de Signal Catcher.
 *
 * - setEventContext: escenario “solo impresión” — timestamp + estado + ids aleatorios por request.
 * - initImpressionClickFunnel / initFullFunnel: generan una tanda de IDs coherentes antes del embudo.
 * - funnelTimestamp: actualiza solo el timestamp ISO antes de cada POST del embudo (mismo state/session).
 */
const { randomUUID } = require('crypto');

const STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'WA', 'CO', 'GA', 'OH', 'MI'];

function pickState() {
  return STATES[Math.floor(Math.random() * STATES.length)];
}

function setEventContext(context, events, done) {
  context.vars.timestamp = new Date().toISOString();
  context.vars.state = pickState();
  done();
}

function initImpressionClickFunnel(context, events, done) {
  context.vars.impId = randomUUID();
  context.vars.clickId = randomUUID();
  context.vars.funnelState = pickState();
  context.vars.sessionId = randomUUID();
  done();
}

function initFullFunnel(context, events, done) {
  context.vars.impId = randomUUID();
  context.vars.clickId = randomUUID();
  context.vars.convId = randomUUID();
  context.vars.funnelState = pickState();
  context.vars.sessionId = randomUUID();
  done();
}

function funnelTimestamp(context, events, done) {
  context.vars.timestamp = new Date().toISOString();
  context.vars.state = context.vars.funnelState;
  done();
}

module.exports = {
  setEventContext,
  initImpressionClickFunnel,
  initFullFunnel,
  funnelTimestamp,
};
