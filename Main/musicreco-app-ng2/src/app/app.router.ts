import { ModuleWithProviders } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';

import { AppComponent } from './app.component';
import { LoginComponent } from './login/login.component';
import { SignupComponent } from './signup/signup.component';
import { HomeComponent } from './home/home.component';
import { UserPreferenceComponent } from './user-preference/user-preference.component';
import { RecommendationsComponent } from './recommendations/recommendations.component';


export const router: Routes = [
    {path: '',redirectTo:'', pathMatch: 'full'},
    {path: 'home', component: HomeComponent},
    {path: 'signup',component: SignupComponent},
    {path: 'login',component: LoginComponent},
    {path: 'userpreference',component: UserPreferenceComponent},
    {path: 'recommendations', component: RecommendationsComponent}
];

export const routes: ModuleWithProviders = RouterModule.forRoot(router); 
