import {Routes, Router, RouterModule} from '@angular/router';
import {LoginComponent} from './login/login.component';
import {RecommendationsComponent} from './recommendation/reco.component'
import {ArtistpageComponent} from './artist/artist.component'
import {SignupComponent} from './signup/signup.component'
import {ColdStartComponent} from './recommendation/coldstart.component'
 import { UserPreferenceComponent} from './preference/userpreference.component'



const APP_ROUTES : Routes = [
     {path : '', redirectTo : '/login' , pathMatch : 'full'},
     {path : 'login', component : LoginComponent},
     {path : 'signup', component : SignupComponent},
     {path : 'recommendation/:id', component : RecommendationsComponent},
     {path : 'artist/:id/:artistName', component : ArtistpageComponent},
    {path : 'userpreference/:id', component : UserPreferenceComponent},
       {path : 'coldstart/:id/:artist', component : ColdStartComponent}
]

export const routing = RouterModule.forRoot(APP_ROUTES);
