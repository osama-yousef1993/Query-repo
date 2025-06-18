import { inject } from '@angular/core';
import { CanActivateChildFn, Router } from '@angular/router';
import { AuthService } from './auth.service';

export const authGuard = (location: string, verb: string): CanActivateChildFn => (() => {
  const router = inject(Router);
  const authService = inject(AuthService);

  const result = (async () => {
    const hasAccess = await authService.hasVerb(location, verb);

    // if (!hasAccess) {
    //   router.navigate(['fuel/auth-required']);
    //   return false;
    // }

    return true;
  })();

  return result;
});
