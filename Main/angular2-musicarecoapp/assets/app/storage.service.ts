import {Injectable} from '@angular/core';
import {Headers, Http, Response} from '@angular/http';
import 'rxjs/Rx';

@Injectable()
export class StorageService {

    constructor(private http : Http) {}


usersession : string[] = []

public addSong(song : string) {
this.usersession.push(song)
}

public getSession()  {
    return this.usersession;
}
    
}