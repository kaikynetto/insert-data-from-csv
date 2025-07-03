import fs from 'fs';
import fetch from 'node-fetch';
import { Client } from 'pg';
import csvParser from 'csv-parser';
import 'dotenv/config';
import schedule from 'node-schedule';

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
  console.log('Iniciando importação...');

  const driveFileId = '11rDoORTnsANZubyxC1BzUKLpmRRn0My9';
  const downloadUrl = `https://drive.google.com/uc?export=download&id=${driveFileId}`;

  const response = await fetch(downloadUrl);
  if (!response.ok) throw new Error('Falha ao baixar o CSV.');

  const fileBuffer = Buffer.from(await response.arrayBuffer());
  fs.writeFileSync('report.csv', fileBuffer);

  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });

  await client.connect();

  const results = [];
  const today = new Date();
  const todayStr = `${today.getDate().toString().padStart(2, '0')}/${(today.getMonth() + 1).toString().padStart(2, '0')}/${today.getFullYear()}`;

  fs.createReadStream('report.csv')
    .on('error', (err) => {
      console.error('Erro ao abrir o arquivo:', err);
    })
    .pipe(csvParser({ separator: ',', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (data) => {
      const reportDate = data['Data Relatório']?.replace(/"/g, '').trim();
      if (reportDate === todayStr) {
        const row = {
          report_date: convertDate(data['Data Relatório']),
          century: data['Century'],
          player: data['Player'] || null,
          network: data['Network'] || null,
          name: data['Name'] || null,
          currency: data['Currency'] || null,
          buy_in: safeParseFloat(data['Buy in']),
          profit: safeParseFloat(data['Profit']),
          shots: data['Shots'] ? safeParseInt(data['Shots']) : 0,
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
      }
    })
    .on('end', async () => {
      console.log(`Total de registros para inserir: ${results.length}`);
      try {
        for (const row of results) {
          const missingFields = [];
          if (!row.report_date) missingFields.push('report_date');
          if (row.profit === null || row.profit === undefined) missingFields.push('profit');
          if (!row.date) missingFields.push('date');
          if (!row.time) missingFields.push('time');

          if (missingFields.length > 0) {
            console.log(`Ignorado por falta de campos: tournament_id ${row.tournament_id}, player ${row.player}`);
            continue;
          }

          const res = await client.query(
            'SELECT 1 FROM lucas."MERGED_AUDITORIA" WHERE tournament_id = $1 AND player = $2 LIMIT 1',
            [row.tournament_id, row.player]
          );

          if (res.rowCount > 0) {
            console.log(`Duplicado, não inserido: tournament_id ${row.tournament_id}, player ${row.player}`);
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
      } catch (err) {
        console.error('Erro na importação:', err);
      } finally {
        await client.end();
        console.log('Importação finalizada.');
      }
    });
}
console.log("running file")
schedule.scheduleJob('00 07 * * *', function () {
    main();
});