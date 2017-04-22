import { Component } from '@angular/core';
import {StorageService} from './storage.service';

@Component({
    selector: 'my-app',
    templateUrl: './app.component.html',
    providers : [StorageService]
})
export class AppComponent {
    
}