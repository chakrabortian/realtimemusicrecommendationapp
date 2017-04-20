import { MusicRecommendationPage } from './app.po';

describe('music-recommendation App', () => {
  let page: MusicRecommendationPage;

  beforeEach(() => {
    page = new MusicRecommendationPage();
  });

  it('should display message saying app works', () => {
    page.navigateTo();
    expect(page.getParagraphText()).toEqual('app works!');
  });
});
