# coding: utf-8

import os
import pandas as pd
from serpapi_consumer.consumer import SerpApiSearcher, ResultFormater, PageContentScraper, SharedCountConsumer, DomainCounter

class Pipeline:
    """
    Executa o pipeline de buscas, formatação, extração de conteúdo e contagem de compartilhamentos

    Exemplo
    --------
    >> from serp_api_consumer.pipeline import Pipeline
    >> pipeline = Pipeline(config=config)
    >> pipeline.run(count_sharing=True, extract_content=True)
    """
    def __init__(self, config, serpapi_key=None, sharedcount_key=None):
        self.config = config
        self._serpapi_key = serpapi_key
        self._sharedcount_key = sharedcount_key

    def run_searches(self):
        assert self.config, "A configuração da busca é um parâmetro requerido, esta pode ser um dicionário ou o caminho de um arquivo json"

        searcher = SerpApiSearcher(config=self.config, serpapi_key=self._serpapi_key)
        print('running searches')
        searcher.run()

        raw_results = searcher.results
        self.raw_results_ = raw_results

        #formatação dos resultados
        print('formating results')
        formater = ResultFormater()
        searches_df, results_df = formater.format_results(raw_results, searcher.tbm)

        self.searches_df_ = searches_df
        self.results_df_ = results_df
        self.unique_results_links_ = list(results_df.link.unique())

        searches_df.to_csv("searches.csv", index=False)
        results_df.to_csv("results.csv", index=False)

    def extract_results_content(self):
        #extração do conteúdo dos links nos resultados
        print('extracting content')
        content_scraper = PageContentScraper()
        content_scraper.load_urls(self.unique_results_links_) #usando os links únicos obtidos no passo anterior
        content_scraper.run()

        #dataframe com os links e conteúdos
        content_df = pd.DataFrame(content_scraper.results)
        self.content_df_ = content_df

        content_df.to_csv("content.csv", index=False)

    def count_link_sharing(self):
        #contagem de compartilhamento com sharedcount
        #o Shared count possui um limite de 500 requisições
        print('getting shared count')
        shared_count = SharedCountConsumer(sharedcount_key=self._sharedcount_key)
        shared_count.load_urls(self.unique_results_links_)
        shared_count.run()

        shared_results = shared_count.get_formated_results()
        shared_df = pd.DataFrame(shared_results)
        self.shared_df_ = shared_df

        shared_df.to_csv("shared.csv", index=False)

    def count_domains(self):
        if self.content_df_ is not None:
            domain_counter = DomainCounter(self.results_df_, self.content_df_)
        else:
            domain_counter = DomainCounter(self.results_df_)
        domain_counter.main()
        domains_df = domain_counter.domains_df_
        self.domains_df_ = domains_df

        domains_df.to_csv("domains.csv", index=False)

    def run(self, extract_content=False, count_sharing=False):
        #cobre o caso onde o count_sharing é True, mas a chave do sharedcount não foi passada e nem configurada como variável de ambiente
        if count_sharing and not(self._sharedcount_key or "SHAREDCOUNT_KEY" in os.environ):
            raise Exception("Para realizar a contagem de compartilhamentos, adicione a variável de ambiente SHAREDCOUNT_KEY ou forneça o parâmetro sharedcount_key")
        
        self.run_searches()
        if extract_content:
            self.extract_results_content()
        else:
            self.content_df_ = None

        self.count_domains()

        if count_sharing:
            self.count_link_sharing()