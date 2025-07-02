import fs from 'fs';
import { Client } from 'pg';
import csvParser from 'csv-parser';
import 'dotenv/config';

function safeParseInt(value) {
  const n = parseInt(value);
  return Number.isNaN(n) ? null : n;
}

function safeParseFloat(value) {
  const n = parseFloat(value.replace(/[^\d.-]/g, ''));
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
  const client = new Client({
    connectionString: process.env.DATABASE_URL
  });

  await client.connect();

  const results = [];

  fs.createReadStream('sharkbot.csv')
    .pipe(csvParser({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
    .on('data', (data) => {
      const row = {
        search_date: convertDate(data['Data Pesquisa']),
        nickname: data['Nick'] || null,
        tournaments_count: safeParseInt(data['Qtd Torneios']),
        average_stack: safeParseFloat(data['Stack Medio']),
        profit: safeParseFloat(data['Lucro']),
        bot_date: data['Data BOT']?.trim() || null,
      };
      results.push(row);
    })
    .on('end', async () => {
      for (const row of results) {
        try {
          const exists = await client.query(
            `SELECT 1 FROM lucas."sharkbot"
             WHERE nickname = $1 AND search_date = $2 AND bot_date = $3 LIMIT 1`,
            [row.nickname, row.search_date, row.bot_date]
          );

          if (exists.rowCount > 0) {
            console.log(`⚠️ Ignorado (duplicado): ${row.nickname} | ${row.search_date} | ${row.bot_date}`);
            continue;
          }

          await client.query(
            `INSERT INTO lucas."sharkbot" (
              search_date, nickname, tournaments_count, average_stack, profit, bot_date
            ) VALUES ($1, $2, $3, $4, $5, $6)`,
            [
              row.search_date,
              row.nickname,
              row.tournaments_count,
              row.average_stack,
              row.profit,
              row.bot_date
            ]
          );

          console.log(`Inserido: ${row.nickname} | ${row.search_date} | ${row.bot_date}`);
        } catch (err) {
          console.error(`Erro ao inserir ${row.nickname}:`, err.message);
        }
      }

      await client.end();
    });
}

main();
