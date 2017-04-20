import { Injectable, EventEmitter } from '@angular/core';
import { User } from './user';
import 'rxjs/Rx';
import { Headers, Http, Response } from '@angular/http';

@Injectable()
export class UserService {
    usersAdded = new EventEmitter<User[]>();
    private users: User[] = [
        new User('harry', 'potter')
    ];
    constructor(private http: Http) {}

    getUsers(){
        return this.users;
    }

    getUser(username){
        return this.users[username];
    }

    addUser(user: User){
        this.users.push(user);
    }
}