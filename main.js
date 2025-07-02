import fs from 'fs';
import { Client } from 'pg';
import csvParser from 'csv-parser';
import 'dotenv/config';

function safeParseInt(value) {
  const n = parseInt(value);
  return Number.isNaN(n) ? null : n;
}

function safeParseFloat(value) {
  const n = parseFloat(value);
  return Number.isNaN(n) ? 0 : n;
}

function convertDate(str) {
  if (!str) return null;
  str = str.replace(/"/g, '').trim();
  const [day, month, year] = str.split('/');
  if (!day || !month || !year) return null;
  return `${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}`;
}

async function main() {
    console.log("asas")
  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });

  await client.connect();

  const results = [];

  fs.createReadStream('report.csv')
    .pipe(csvParser({ separator: ',', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (data) => {
      const row = {
        report_date: convertDate(data['Data Relatório']),
        century: data['Century'],
        player: data['Player'] || null,
        network: data['Network'] || null,
        name: data['Name'] || null,
        currency: data['Currency'] || null,
        buy_in: safeParseFloat(data['Buy in']),
        profit: safeParseFloat(data['Profit']),
        shots: data['Shots'] ? safeParseInt(data['Shots']) : null,
        date: data['Date'] ? data['Date'].replace(/"/g, '').trim() : null,
        time: data['Time'] ? data['Time'].replace(/"/g, '').trim() : null,
        total_entrants: safeParseInt(data['Total Entrants']),
        tournament_id: data['Tournament ID'] || null,
        stake: data['Stake'] || null,
        game: data['Game'] || null,
        structure: data['Structure'] || null,
        flags: data['Flags'] || null,
        rake: safeParseFloat(data['Rake']),
        position: safeParseInt(data['Position']),
        speed: data['Speed'] || null
      };
      results.push(row);
    })
    .on('end', async () => {
      const notInserted = [];

      try {
        for (const row of results) {
          if (!row.report_date || !row.profit || row.shots === null || !row.date || !row.time) {
            notInserted.push({
              row,
              reason: 'Campos obrigatórios faltando (report_date, profit, shots, date ou time)'
            });
            continue;
          }

          const res = await client.query(
            'SELECT 1 FROM lucas."MERGED_AUDITORIA" WHERE tournament_id = $1 AND player = $2 LIMIT 1',
            [row.tournament_id, row.player]
          );

          if (res.rowCount > 0) {
            notInserted.push({
              row,
              reason: `Registro já existe para tournament_id ${row.tournament_id} e player ${row.player}`
            });
            continue;
          }

          await client.query(
            `INSERT INTO lucas."MERGED_AUDITORIA" (
              report_date, century, player, network, name, currency, buy_in, profit, shots, date, time, total_entrants, tournament_id, stake, game, structure, flags, rake, position, speed
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
            )`,
            [
              row.report_date,
              row.century,
              row.player,
              row.network,
              row.name,
              row.currency,
              row.buy_in,
              row.profit,
              row.shots,
              row.date,
              row.time,
              row.total_entrants,
              row.tournament_id,
              row.stake,
              row.game,
              row.structure,
              row.flags,
              row.rake,
              row.position,
              row.speed
            ]
          );

          console.log(`Inserido: tournament_id ${row.tournament_id}, player ${row.player}`);
        }

        console.log('\nRegistros NÃO inseridos:');
        for (const fail of notInserted) {
          console.log(`Tournament_id: ${fail.row.tournament_id}, Player: ${fail.row.player}, Motivo: ${fail.reason}`);
        }
      } finally {
        await client.end();
      }
    });
}

main();
