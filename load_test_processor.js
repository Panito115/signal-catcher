/**
 * Artillery hooks: timestamps alineados al brief y estado variado para
 * dimensionar correctamente los agregados (p. ej. top estados en Grafana).
 */
const STATES = ['CA', 'NY', 'TX', 'FL', 'IL', 'WA', 'CO', 'GA', 'OH', 'MI'];

function setEventContext(context, events, done) {
  context.vars.timestamp = new Date().toISOString();
  context.vars.state = STATES[Math.floor(Math.random() * STATES.length)];
  done();
}

module.exports = { setEventContext };
