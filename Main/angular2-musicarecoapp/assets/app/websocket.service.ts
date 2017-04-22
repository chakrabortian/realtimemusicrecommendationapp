import { Injectable } from '@angular/core';
import {Subject, Observable, Observer} from 'rxjs/Rx';



@Injectable()
export class WebSocketService {

  private subject : Subject<MessageEvent>

  constructor() { }

  public connect(url: string): Subject<MessageEvent>  {
      console.log("Sending web socket connection request")
    let ws = new WebSocket(url);

    let observable = Observable.create((obs: Observer<MessageEvent>) => {
            ws.onmessage = obs.next.bind(obs);
            ws.onerror = obs.error.bind(obs);
            ws.onclose = obs.complete.bind(obs);
            console.log('Binding now')
            return ws.close.bind(ws);
        });

    let observer = {
            next: (data: Object) => {
                console.log('Established connection', data)
                if (ws.readyState === WebSocket.OPEN) {
                    console.log('Established connection')
                    ws.send(data);
                }
            },
        };

        return Subject.create(observer, observable);
  }


  

}
