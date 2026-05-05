import { NextResponse } from 'next/server';
import net from 'net';

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { host = '127.0.0.1', port = 7070, username, password, ...command } = body;
    
    return new Promise<Response>((resolve) => {
      const client = new net.Socket();
      let responseData = '';
      let isAuthenticating = !!(username && password);
      
      client.connect(Number(port), host, () => {
        if (isAuthenticating && command.action !== 'authenticate') {
            client.write(JSON.stringify({ action: 'authenticate', username, password }) + '\n');
        } else {
            client.write(JSON.stringify(command) + '\n');
        }
      });

      client.on('data', (data) => {
        responseData += data.toString();
        
        const lines = responseData.trim().split('\n');
        
        if (lines.length > 0) {
          try {
            const lastLine = lines[lines.length - 1];
            const json = JSON.parse(lastLine);
            
            if (json.server === "LumeDB" && json.message?.includes("Welcome")) {
                if (lines.length > 1) {
                    const actualResponse = JSON.parse(lines[1]);
                    if (isAuthenticating && command.action !== 'authenticate' && actualResponse.status === 'ok' && actualResponse.message === 'Authentication successful') {
                        isAuthenticating = false;
                        responseData = ''; // reset for next response
                        client.write(JSON.stringify(command) + '\n');
                        return;
                    }
                    client.destroy();
                    resolve(NextResponse.json(actualResponse));
                }
                return;
            }
            
            if (isAuthenticating && command.action !== 'authenticate') {
                if (json.status === 'ok' && json.message === 'Authentication successful') {
                    isAuthenticating = false;
                    responseData = ''; // reset for next response
                    client.write(JSON.stringify(command) + '\n');
                    return;
                } else if (json.status === 'error') {
                    client.destroy();
                    resolve(NextResponse.json(json));
                    return;
                }
            }

            client.destroy();
            resolve(NextResponse.json(json));
          } catch (e) {
            // Incomplete JSON, wait for more data
          }
        }
      });

      client.on('error', (err) => {
        client.destroy();
        resolve(NextResponse.json({ status: 'error', error: 'Failed to connect to LumeDB. Is it running on port 7070?' }, { status: 500 }));
      });
      
      client.on('timeout', () => {
        client.destroy();
        resolve(NextResponse.json({ status: 'error', error: 'Connection timed out' }, { status: 504 }));
      });
      
      client.setTimeout(5000);
    });
  } catch (err: any) {
    return NextResponse.json({ status: 'error', error: err.message }, { status: 400 });
  }
}
