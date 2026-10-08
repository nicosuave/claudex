// Run with bun; extracts the installed desktop's final turn request expression.
// The state below supplies inputs to that expression, not a rewritten payload.
import fs from 'node:fs';
import crypto from 'node:crypto';

const path = process.argv[2];
if (!path) throw new Error('Pass the extracted app-shared JavaScript bundle');
const source = fs.readFileSync(path, 'utf8');
const builderStart = source.indexOf('function zln(');
const start = source.indexOf('let ze={threadId:', builderStart);
const end = source.indexOf(';e.logger.info(', start);
if (builderStart < 0 || start < 0 || end < 0) throw new Error('Desktop builder changed; inspect before regenerating');
const expression = source.slice(start + 'let ze='.length, end);
if (!expression.endsWith('}')) throw new Error('Unexpected request expression boundary');
const state = {
  s: {input:[{type:'text',text:'desktop payload',text_elements:[]}],turnTrigger:'submit',disabledPluginIds:[]},
  t:'$THREAD_ID', r:'desktop-client-message', i:{}, a:{}, D:null,
  je:null, Me:false, de:'$CWD', Se:true, xe:true, R:'on-request',
  re:false, le:'user', Ce:null, Oe:true, ve:{type:'dangerFullAccess'}, Ae:['$CWD'],
  F:'fable', Ie:null, ee:'max', Kln:'explicitRequestOnly', Re:'detailed', Le:'pragmatic',
  A:{mode:'default',settings:{model:'fable',reasoning_effort:'max',developer_instructions:null}}, T:'project',
  // No search MCP context is supplied in this fixture; this is trn's first branch.
  trn:(metadata, context)=>{if(context != null) throw new Error('Unexpected search context'); return metadata;},
};
const evaluate = new Function(...Object.keys(state), `return (${expression});`);
const request = JSON.parse(JSON.stringify(evaluate(...Object.values(state)), (_, v)=>v === undefined ? null : v));
const fixture = {
  source: path.split('/').at(-1),
  expressionSha256:crypto.createHash('sha256').update(expression).digest('hex'),
  request,
};
fs.writeFileSync('tests/fixtures/desktop-turn.json', JSON.stringify(fixture,null,2)+'\n');
console.log(`Extracted ${Object.keys(request).length} turn fields from zln -> Xln -> run -> UNt -> turn/start`);
