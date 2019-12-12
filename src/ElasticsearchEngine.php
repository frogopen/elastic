<?php 

namespace Frogopen\Elastic;

use Laravel\Scout\Builder;
use Elasticsearch\Client as Elastic;
use Laravel\Scout\Engines\Engine;

class ElasticsearchEngine extends Engine
{

	/**
     * Index where the models will be saved.
     *
     * @var string
     */
    protected $index;
    
    /**
     * Elastic where the instance of Elastic|\Elasticsearch\Client is stored.
     *
     * @var object
     */
    protected $elastic;
    /**
     * Create a new engine instance.
     *
     * @param  \Elasticsearch\Client  $elastic
     * @return void
     */
    public function __construct(Elastic $elastic, $index)
    {
        $this->elastic = $elastic;
        $this->index = $index;
    }

	public function update($models)
	{
        $params['body'] = [];
        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'update' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    // '_type' => $model->searchableAs(),
                ]
            ];
            $params['body'][] = [
                'doc' => $model->toSearchableArray(),
                'doc_as_upsert' => true
            ];
        });
        $this->elastic->bulk($params);
	}

	public function delete($models)
	{
        $params['body'] = [];
        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                    // '_type' => $model->searchableAs(),
                ]
            ];
        });
        $this->elastic->bulk($params);
	}

	public function search(Builder $builder)
	{
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'size' => $builder->limit,
        ]));
	}

	public function paginate(Builder $builder, $perPage, $page)
	{
		$result = $this->performSearch($builder, [
            'numericFilters' => $this->filters($builder),
            'from' => (($page * $perPage) - $perPage),
            'size' => $perPage,
        ]);
       	$result['nbPages'] = $result['hits']['total']['value'] / $perPage;
        return $result;
	}

	public function mapIds($results)
	{
        return collect($results['hits']['hits'])->pluck('_id')->values();
	}

	public function map(Builder $builder, $results, $model)
	{
		if ($results['hits']['total']['value'] === 0) {
            return $model->newCollection();
        }
        $keys = collect($results['hits']['hits'])->pluck('_id')->values()->all();
        return $model->getScoutModelsByIds(
                $builder, $keys
            )->filter(function ($model) use ($keys) {
                return in_array($model->getScoutKey(), $keys);
            });
	}

	public function getTotalCount($results)
	{
        return $results['hits']['total'];
	}

	public function flush($model)
	{
        $model->newQuery()
            ->orderBy($model->getKeyName())
            ->unsearchable();
	}
}