require('dotenv/config');

const { google } = require('googleapis');

const DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive';
const DEFAULT_REDIRECT_URI = 'http://127.0.0.1:3005/oauth2callback';

function getOAuthClient() {
  const clientId = process.env.GOOGLE_OAUTH_CLIENT_ID?.trim();
  const clientSecret = process.env.GOOGLE_OAUTH_CLIENT_SECRET?.trim();
  const redirectUri =
    process.env.GOOGLE_OAUTH_REDIRECT_URI?.trim() || DEFAULT_REDIRECT_URI;

  if (!clientId || !clientSecret) {
    throw new Error(
      'Missing GOOGLE_OAUTH_CLIENT_ID or GOOGLE_OAUTH_CLIENT_SECRET in .env',
    );
  }

  return new google.auth.OAuth2(clientId, clientSecret, redirectUri);
}

async function printAuthorizationUrl() {
  const oauth2Client = getOAuthClient();
  const url = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    prompt: 'consent',
    include_granted_scopes: true,
    scope: [DRIVE_SCOPE],
  });

  console.log(
    'Open this URL in your browser and authorize Google Drive access:\n',
  );
  console.log(url);
  console.log(
    '\nAfter the redirect, copy the "code" query parameter and run:\n' +
      'npm run drive:oauth:token -- YOUR_AUTHORIZATION_CODE',
  );
}

async function exchangeCodeForTokens(code) {
  if (!code) {
    throw new Error(
      'Missing authorization code. Usage: npm run drive:oauth:token -- YOUR_AUTHORIZATION_CODE',
    );
  }

  const oauth2Client = getOAuthClient();
  const { tokens } = await oauth2Client.getToken(code);

  console.log('Tokens received:\n');
  console.log(JSON.stringify(tokens, null, 2));

  if (tokens.refresh_token) {
    console.log(
      '\nSet this value in .env as GOOGLE_OAUTH_REFRESH_TOKEN:\n' +
        tokens.refresh_token,
    );
  } else {
    console.log(
      '\nGoogle did not return a refresh_token. Revoke prior consent if needed and retry with prompt=consent.',
    );
  }
}

async function main() {
  const command = process.argv[2];
  const code = process.argv[3];

  if (command === 'url') {
    await printAuthorizationUrl();
    return;
  }

  if (command === 'token') {
    await exchangeCodeForTokens(code);
    return;
  }

  throw new Error(
    'Unknown command. Use "url" or "token". Example: npm run drive:oauth:url',
  );
}

main().catch((error) => {
  console.error(error.message || error);
  process.exit(1);
});
