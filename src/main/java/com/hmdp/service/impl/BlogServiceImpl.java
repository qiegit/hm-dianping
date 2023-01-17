package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.ScrollResult;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Autowired
    private UserServiceImpl userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IFollowService followService;
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = this.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLike(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(long id) {
//        查询blog
        Blog blog = this.getById(id);
        if (blog == null){
            return Result.fail("博客不存在");
        }
//        查询blog用户
        queryBlogUser(blog);
        isBlogLike(blog);
        return Result.ok(blog);
    }

    /**
     * 查询用户是否点赞并修改blog
     * @param blog
     */
    private void isBlogLike(Blog blog) {
        //        1.获取登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
            return;
        }
        Long userId = UserHolder.getUser().getId();
//        2.判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    @Override
    public Result likeBlog(Long id) {
//        1.获取登录用户
        Long userId = UserHolder.getUser().getId();
//        2.判断当前用户是否已经点赞
        String key = BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if (score == null){
//            3.没有点赞，可以点赞
//            3.1 数据库点赞
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if (isSuccess) {
//            3.2 保存信息到redis
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else {
//            4.如果已经点赞，则取消点赞
//            4.1 数据库取消点赞
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if (isSuccess) {
//            4.2 redis删除用户
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(long id) {
//        1. 查询top5的点赞用户
        String ket = BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(ket, 0, 4);
        if (top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
//        2。 解析用户id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
//      3.根据用户id查询用户 WHERE id IN ( 5 , 1 ) ORDER BY FIELD(id, 5, 1)
        String idStr = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService.query().in("id",ids).last("order by field(id," + idStr +")").list()
                .stream().map(user ->
                    BeanUtil.copyProperties(user, UserDTO.class)
                ).collect(Collectors.toList());

//        4. 返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);
        if (!isSuccess){
            return Result.fail("新增博文失败");
        }
//        1.查询博主所有粉丝  select * from tb_follow where follow_user_id = ?
        List<Follow> followUserId = followService.query().eq("follow_user_id", user.getId()).list();
//        2.推送博文给所有分析
        for (Follow follow : followUserId) {
//            2.1获取粉丝id
            Long userId = follow.getUserId();
//            2.2推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }

        // 返回id
        return Result.ok(blog.getId());
    }

    /**
     * 根据
     * @param max  上一次查询的最小时间戳
     * @param offset  重复时间戳的偏移量
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
//        1.查询当前用户
        Long userId = UserHolder.getUser().getId();
//        2.查询收件箱 ZREVRANGEBYSCORE key Max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);

//        3.解析数据 博客blogId,下次查询最小值max,重复偏移量offset
        assert typedTuples != null;
        if (typedTuples.isEmpty()){
            return Result.ok();
        }
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {  // 5 4 4 2 2
//            3.1获取id
            ids.add(Long.valueOf(typedTuple.getValue()));
//            3.2获取分数(最小时间戳)
            long time = typedTuple.getScore().longValue();
//            3.3获取offset
            if (minTime == time){
                os++;
            }else {
                os =1;
                minTime = time;
            }
        }
        os = minTime == max ? os : os+offset;

//        4.查询blog
        String idStr = StrUtil.join(",",ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idStr + ")").list();
        for (Blog blog : blogs) {
            // 1.查询blog有关的用户
            queryBlogUser(blog);
//            2.查询blog是否被点赞
            isBlogLike(blog);
        }

//        5.封装返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setOffset(os);
        scrollResult.setMinTime(minTime);

        return Result.ok(scrollResult);
    }

    /**
     * 查询博客用户信息并添加到blog
     * @param blog
     */
    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
