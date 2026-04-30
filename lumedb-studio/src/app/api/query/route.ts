import { NextResponse } from 'next/server';
import net from 'net';

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const { host = '127.0.0.1', port = 7070, ...command } = body;
    
    return new Promise<Response>((resolve) => {
      const client = new net.Socket();
      let responseData = '';
      
      // Connect to the provided LumeDB host/port
      client.connect(Number(port), host, () => {
        client.write(JSON.stringify(command) + '\n');
      });

      client.on('data', (data) => {
        responseData += data.toString();
        
        // LumeDB sends JSON responses ending with newlines. 
        // We split by newline. The first might be the welcome message.
        const lines = responseData.trim().split('\n');
        
        // Find the actual response to our command. 
        // If there's multiple lines, the last one is likely our response.
        if (lines.length > 0) {
          try {
            // Process the last line received
            const lastLine = lines[lines.length - 1];
            const json = JSON.parse(lastLine);
            
            // If it's just the welcome message, wait for the next data
            if (json.server === "LumeDB" && json.message?.includes("Welcome")) {
                if (lines.length > 1) {
                    // we already got our response too
                    const actualResponse = JSON.parse(lines[1]);
                    client.destroy();
                    resolve(NextResponse.json(actualResponse));
                }
                // otherwise keep waiting
                return;
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
      
      // Set a timeout of 5 seconds
      client.setTimeout(5000);
    });
  } catch (err: any) {
    return NextResponse.json({ status: 'error', error: err.message }, { status: 400 });
  }
}
