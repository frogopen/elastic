<?php 

namespace Frogopen\Elastic;

use Laravel\Scout\Builder;
use Elasticsearch\Client as Elastic;
use Laravel\Scout\Engines\Engine;
use Illuminate\Database\Eloquent\Collection;
use Illuminate\Support\Collection as BaseCollection;

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

    /**
     * Update the given model in the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function update($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'update' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                ]
            ];
            $params['body'][] = [
                'doc' => $model->toSearchableArray(),
                'doc_as_upsert' => true
            ];
        });
        $this->elastic->bulk($params);
	}

    /**
     * Remove the given model from the index.
     *
     * @param  Collection  $models
     * @return void
     */
    public function delete($models)
    {
        $params['body'] = [];

        $models->each(function($model) use (&$params)
        {
            $params['body'][] = [
                'delete' => [
                    '_id' => $model->getKey(),
                    '_index' => $this->index,
                ]
            ];
        });

        $this->elastic->bulk($params);
    }

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @return mixed
     */
    public function search(Builder $builder)
   	{
        return $this->performSearch($builder, array_filter([
            'numericFilters' => $this->filters($builder),
            'size' => $builder->limit,
        ]));
    }
    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  int  $perPage
     * @param  int  $page
     * @return mixed
     */
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

    /**
     * Perform the given search on the engine.
     *
     * @param  Builder  $builder
     * @param  array  $options
     * @return mixed
     */
    protected function performSearch(Builder $builder, array $options = [])
    {
        $params = [
            'index' => $this->index,
            'body' => [
                'query' => [
                    'bool' => [
                        'must' => [['query_string' => [ 'query' => "*{$builder->query}*"]]]
                    ]
                ]
            ]
        ];

        if ($sort = $this->sort($builder)) {
            $params['body']['sort'] = $sort;
        }

        if (isset($options['from'])) {
            $params['body']['from'] = $options['from'];
        }

        if (isset($options['size'])) {
            $params['body']['size'] = $options['size'];
        }
        //     // if(isset($options['numericFilters'][0]['query_string'])) {
        //     //     $params['body']['query']['bool']['must'][0]['query_string']['fields'] = $options['numericFilters'][0]['query_string'];
        //     //     if (isset($options['numericFilters'][1]['match_phrase'])) {
        //     //     	$params['body']['query']['bool']['must'][1]['match_phrase'] = $options['numericFilters'][1]['match_phrase'];
        //     //     }
        //     // } else {
        //     //     $params['body']['query']['bool']['must'] = array_merge($params['body']['query']['bool']['must'],
        //     //         $options['numericFilters']);
        //     // }
        // }
        if (isset($options['numericFilters']) && count($options['numericFilters'])) {
         	if(isset($options['numericFilters'])) {
	          	foreach($options['numericFilters'] as $k => $v) {
	           		foreach($v as $kk => $vv) {
			            if($kk == 'query_string') {
			             	$params['body']['query']['bool']['must'][$k][$kk]['fields'] = $options['numericFilters'][$k][$kk];
			            } else {
			             	$params['body']['query']['bool']['must'][$k][$kk] = $options['numericFilters'][$k][$kk];
			            }
		           }
	          }
         } else {
            $params['body']['query']['bool']['must'] = array_merge($params['body']['query']['bool']['must'],
                $options['numericFilters']);
            }
        }
        if ($builder->callback) {
            return call_user_func(
                $builder->callback,
                $this->elastic,
                $builder->query,
                $params
            );
        }
        return $this->elastic->search($params);
    }

    /**
     * Get the filter array for the query.
     *
     * @param  Builder  $builder
     * @return array
     */
    protected function filters(Builder $builder)
    {
        return collect($builder->wheres)->map(function ($value, $key) {
            if (is_array($value) && $key != 'query') {
                return ['terms' => [$key => $value]];
            }
            if ($key == 'query') {
                return ['query_string' => $value];
            }
            return ['match_phrase' => [$key => $value]];
        })->values()->all();
    }

    /**
     * Pluck and return the primary keys of the given results.
     *
     * @param  mixed  $results
     * @return \Illuminate\Support\Collection
     */
	public function mapIds($results)
	{
        return collect($results['hits']['hits'])->pluck('_id')->values();
	}

    /**
     * Map the given results to instances of the given model.
     *
     * @param  \Laravel\Scout\Builder  $builder
     * @param  mixed  $results
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return Collection
     */
	public function map(Builder $builder, $results, $model)
	{
		if ($results['hits']['total']['value'] === 0) {
            return $model->newCollection();
        }
        $keys = collect($results['hits']['hits'])->pluck('_id')->values()->all();
        //如果是俩个表联合查询，返回下面的代码，分页数据必须是一个集合。
        // $result = [];
        // foreach (collect($results['hits']['hits']) as $key => $value) {
        //     $result[] = $value['_source'];
        // }
        // return collect($result);

        // return $result;
        // $models = $model->whereIn(
        //     $model->getKeyName(), $keys
        // )->get()->keyBy($model->getKeyName());
        // return collect($results['hits']['hits'])->map(function ($hit) use ($model, $models) {
        //     $one = $models[$hit['_id']];
        //     return $one;
        // });
        return $model->getScoutModelsByIds(
                $builder, $keys
            )->filter(function ($model) use ($keys) {
                return in_array($model->getScoutKey(), $keys);
            });
	}

    /**
     * Get the total count from a raw result returned by the engine.
     *
     * @param  mixed  $results
     * @return int
     */
	public function getTotalCount($results)
	{
        return $results['hits']['total']['value'];
	}

    /**
     * Flush all of the model's records from the engine.
     *
     * @param  \Illuminate\Database\Eloquent\Model  $model
     * @return void
     */
    public function flush($model)
    {
        $model->newQuery()
            ->orderBy($model->getKeyName())
            ->unsearchable();
    }

    /**
     * Generates the sort if theres any.
     *
     * @param  Builder $builder
     * @return array|null
     */
    protected function sort($builder)
    {
        if (count($builder->orders) == 0) {
            return null;
        }

        return collect($builder->orders)->map(function($order) {
            return [$order['column'] => $order['direction']];
        })->toArray();
    }
}